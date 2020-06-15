# Dask Cook Backend API

## Motivation

As part of our overall effort to introduce support for Dask workloads with GPU
support in Kubernetes, we would like to be able to deploy jobs using the Dask
API onto Cook. Although [`dask-kubernetes`](https://kubernetes.dask.org/en/latest)
exists and some exploration has been done on that front, using Cook as a
compute backend offers many benefits since it can federate access to multiple
Kubernetes clusters.

## Goals and Non-Goals

This API aims to be as much of a "friction-free" environment for deploying Dask
workloads on Cook as possible. As such, its primary goal is to be able to use
the Cook backend in a "plug-and-play" fashion similar to other backends in
Dask.

Ideally, the process of using Cook on Dask should look something like using
Kubernetes on Dask, as shown below (taken from [here](https://docs.dask.org/en/latest/setup/kubernetes.html)):

```python
from dask_kubernetes import KubeCluster
cluster = KubeCluster.from_yaml('worker-template.yaml')
cluster.scale(20)  # add 20 workers
cluster.adapt()    # or create and destroy workers dynamically based on workload

from dask.distributed import Client
client = Client(cluster)
```

The end result would look something like this:

```python
from dask_cook import CookCluster
cluster = CookCluster('https://my-cook-instance.internal')
cluster.scale(20)  # add 20 workers
cluster.adapt()    # or create and destroy workers dynamically based on workload

from dask.distributed import Client
client = Client(cluster)
```

Or, using context managers:

```python
from dask_cook import CookCluster
from dask.distributed import Client

with CookCluster('https://my-cook-instance.internal') as cluster:
    cluster.scale(20)
    client = Client(cluster)
```

## Architecture

Long story short, a Dask system has two different kinds of nodes: one 
*scheduler node* and several *worker nodes*. The scheduler node will distribute
Dask tasks to worker nodes as it sees fit, whereas the worker nodes will run
these tasks and report back to the scheduler node.

*(This is a simplified explanation of what goes on; refer to "[Journey of a Task](https://distributed.dask.org/en/latest/journey.html)" for a more in-depth look.)*

As with the Kubernetes Dask API, the user would ideally only have to directly
deal with "Dask on Cook"-specific interaction during cluster initialization.
This would be centralized around a `CookCluster` class, which would extend from
the [`SpecCluster`](https://distributed.dask.org/en/latest/api.html#distributed.SpecCluster)
class. We can customize worker and scheduler behavior through that class's
`workers` and `scheduler` parameters, respectively.

### Remote Execution on Cook / The `CookJob` Class

For managing workers, the API will take a similar approach to the Dask on
Kubernetes API. The Dask on Kubernetes API defines a [`Worker`](https://github.com/dask/dask-kubernetes/blob/master/dask_kubernetes/core.py#L115-L138)
class, which extends a generic [`Pod`](https://github.com/dask/dask-kubernetes/blob/master/dask_kubernetes/core.py#L37-L112)
(which allows them to have the scheduler run as its own Pod as well), which
ultimately extends Dask's [`ProcessInterface`](https://distributed.dask.org/en/latest/_modules/distributed/deploy/spec.html) class.
In a similar vein, we will extend `ProcessInterface` into a `CookJob` class,
which we will then extend for our own `Worker` class. Dask provides three
requirements for custom worker types:

1.  An `__await__` method (provided by the `ProcessInterface` superclass, and
    whose behavior we will customize by overriding the `ProcessInterface.start`
    method),
2.  A `close` method (also provided by the `ProcessInterface` superclass and
    whose behavior we will customize by overriding this method),
3.  A `worker_address` property.

The three elements listed above will form the core behavior of our custom
`CookJob` class. An instance of htis class would receive the specifications for
a Cook job in its constructor, corresponding to the signature for
[`JobClient.submit`](https://github.com/twosigma/Cook/blob/master/jobclient/python/cookclient/__init__.py#L96-L111).

#### `async def start(self)`

This function is called when the Cook job is requested to begin. It will submit
a job to the Cook cluster, and periodically poll the cluster until the job's
status switches from `waiting`. If the job status is `running`, then we can
return from the function without a hitch. If the job status is `completed`,
however, then we raise an exception as the job is expected to be a long-running
process.

#### `async def close(self)`

This function is called when a job is being stopped. The current plan is to
first set the worker's state to "`closing`", send a kill job request to Cook,
wait for that to go through, and finally set the state to "`closed`" before
exiting. Unfortunately, this will show the job as "`failed`" from Cook's
perspective. A killswitch approach (wherein a separate process listens on a
port for the signal to exit cleanly) would be nicer, but this would have to
come in later.

#### Monitoring

Each worker instance will spin up a monitoring thread that will do two things.
The thread's first responsibility is to check whether the worker's state is
"`closing`." If the worker's state is "`closing`," then the monitoring thread
is no longer necessary and will exit. The thread will also periodically ping
its corresponding Cook job to ensure that it is still running. If the job is
detected to have stopped, then the thread will make a new job submission to
Cook to spin up a new `dask-worker` instance. This way, we can spin spin the
worker back up if it dies for whatever reason.

### Worker Management / The `Worker` Class

Workers will be implemented as a small wrapper around the `CookJob` class. The
constructor of this method will take the required parameters to submit from the
user. It will also override `start()` to first start the job on Cook, and once
that's running, to fetch the hostname of the running job instance and set the
worker's `worker_address` property to the hostname of the job's running
instance. The end user should never have to see this class; instead, this class
will be managed by the `CookCluster` instance that the user does interact with.

### Scheduler Management

For managing the scheduler, we have decided to start off using Dask's built-in
scheduler, which launches as a separate thread in the same process. The
presence of Python's [Global Interpreter Lock](https://wiki.python.org/moin/GlobalInterpreterLock)
led to considering starting the Dask scheduler as an external process (via the
`dask-scheduler` executable). This would have been running on a Kubernetes
instance. This was ultimately dropped as cleaning up the Kubernetes deployment
on exit would have involved a lot of extra moving parts. Another option would
be to launch the process in the same machine. However, we would end up with a
zombie process if the host process of the API were sent a `SIGKILL` signal.
Thus, a parallel to `dask-kubernetes`'s `Scheduler(Pod)` class will be
explicitly omitted.

A summary of the discovered pros and cons of running the scheduler in-process
versus out-of-process can be found below:

#### In-Process Pros

*   Easy to set up, just instantiate [`LocalCluster`](https://distributed.dask.org/en/latest/api.html#distributed.LocalCluster)
    with the desired arguments.
*   More defined lifecycle &ndash; the scheduler dies at the same time as the
    process.

#### In-Process Cons

*   Scheduler output to stdout and stderr are mixed in with the calling 
    process's output.
*   Subject to Python's GIL limitations. It's not clear how big of a problem
    this actually is because the scheduler seems to be only active when the
    main thread is waiting on I/O anyway.
*   Requires opening ports on the host machine, which may not always be
    feasible.

#### Out-of-Process Pros

*   Can be hosted on a separate machine or on a Kubernetes pod.
*   No risk of running into GIL limitations.
*   Scheduler output and application input remain separate.

#### Out-of-Process Cons

*   More points of failure; e.g. what if the scheduler process fails?
*   Might have to do some manual Kerberos setup.
*   More code to maintain.
*   Kubernetes has quotas in-place.
*   Greater risk of zombies.
*   Cannot terminate a Kubernetes cluster cleanly.

### End-User Interaction / The `CookCluster` Class

The in-depth details of the above shouldn't be exposed to end users. These
details will be abstracted away by the `CookCluster` class, which will extend
`SpecCluster` to set up the scheduler and workers. The `CookCluster` class
would, ideally, only require a selector indicating which Cook address to
connect to at construction time (e.g. https://my-cook-instance.internal), and
then the user can use the standard Dask API via `dask.distributed.Client`.

The end goal will be for deployment of jobs on Cook to be as simple as the
example in [Goals and Non-Goals](#goals-and-non-goals), reproduced below:

```python
from dask_cook import CookCluster
cluster = CookCluster('https://my-cook-instance.internal')
cluster.scale(20)  # add 20 workers
cluster.adapt()    # or create and destroy workers dynamically based on workload

from dask.distributed import Client
client = Client(cluster)
```

With context managers:

```python
from dask_cook import CookCluster
from dask.distributed import Client

with CookCluster('https://my-cook-instance.internal') as cluster:
    cluster.scale(20)
    client = Client(cluster)
```
