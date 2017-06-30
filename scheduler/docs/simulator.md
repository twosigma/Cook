# Cook scheduler faster than real time simulator

The simulator provides a way to audit behavior and test changes to the scheduler.
Given a trace of jobs to run, the hosts available and other configuration, the simulator starts the scheduler connected to a mock of mesos and runs through the trace.
It then produces an output trace of the tasks run.

The simulator operates on a cycle in which it submits new sims, triggers mesos to send completion notices for any tasks that have completed since the last cycle, triggers the scheduler to rank, match, rebalance and all other scheduler processes (max runtime expiry, stragglers, ...). 

The simulator outputs will not account for the time it takes to rank, match or rebalance as time is effectively frozen while each operation is happening. Therefore, the simulator may be used to compare scheduling decisions but not performance changes.

Note, this is different than the "simulator" project at the root of this project. This is a faster than real time simulation of just the scheduling components of cook scheduler. The other simulator is a "system simulator" that stands up cook and all of its dependencies as well as clients which submit jobs. This faster than real time simulator is more useful for understanding and tuning the scheduling algorithms in cook whereas the "system simulator" is more useful for testing and understanding the full system interactions. 

## How to run the simulator

The help message for the simulator run under Cook/scheduler is:

```
lein run -m cook.test.simulator [OPTS]
      --trace-file TRACE_FILE      File of jobs to submit
      --host-file HOST_FILE        File of hosts available in the mesos cluster
      --cycle-step-ms CYCLE_STEP   How much time passes between cycles to move through trace file.
      --out-trace-file TRACE_FILE  File to output trace of tasks run
      --config-file CONFIG_FILE    File in edn format containing config for the simulation
  -h, --help
```

To run the example sim, cd to Cook/scheduler and run:

```
time lein run -m cook.test.simulator --trace-file simulator_files/example-trace.json --host-file simulator_files/example-hosts.json --out-trace-file simulator_files/example-out-trace.csv --config-file simulator_files/example-config.edn
```

### Inputs:

#### trace-file

The trace file contains a list of jobs to run, where each job contains when to submit it, how long it will run, what the completion status should be and other job info.
The other keys for the job mirror the keys in datomic to make it easier to get a trace from a datomic database. The keys used are:

| key | required?  | description  |
|---|---|---|
| run-time-ms  | yes  | how long the job should be allowed to run before it completes and the status provided is used. Note, it may complete sooner if the scheduler kills it for any reason (e.x. preemption) is milliseconds |
| submit-time-ms   | yes  | when to submit the job in milliseconds.The "time" associated with the jobs is arbitrary, though assumed to be in milliseconds. This is to say, shifting all the jobs submit times by some value will not affect the simulation.  |
| status  | yes  | The status mesos will return to cook when the job completes. Accepted values are: ["finished", "failed", "killed", "lost", "error"] |
| job/uuid | yes | uuid of the job |
| job/user | yes | user for the job |
| job/max-retries | yes | number of retries for job |
| job/priority | yes | priority of job |
| job/resource | yes | list of json objects with keys resource/type and resource/amount. This mirrors the cook scheduler schema. Valid types are resource.type/cpus and resource.type/mem |
| job/max-runtime | no | max amount of time in millis to allow the job to run |
| job/name | no | name of the job |
| job/command | no | command of the job (this is ignored!) |
| job/disable-mea-culpa-retries | no | Whether to disable mea culpa retries. Default is false |

An example trace file can be found in Cook/scheduler/simulator_files/example-trace.json

#### host-file

The host file contains a list of hosts that mesos makes available to cook. Here is an example host:

```
{
  "hostname" : "0",
  "attributes" : { },
  "resources" : {
    "cpus" : {
      "*" : 20
    },
    "mem" : {
      "*" : 20000
    },
    "ports" : {
      "*" : [ {
        "begin" : 1,
        "end" : 100
      } ]
    }
  },
  "slave-id" : "bb57002a-75d4-4feb-8695-95e04a1b9c4f"
}
```

The valid resource types are "cpus", "mem" and "ports". An example host file can be found in Cook/scheduler/simulator_files/example-hosts.json

#### cycle-step-ms

The amount of time that passes between cycles.
 
#### config-file

The scheduler-config-file is an edn file for configuring aspects of the scheduler. Currently, the share for users, cycle-step-ms and scheduler-config can be set here. The command line options will take precedence over anything in the config file. 

See an example config file in Cook/scheduler/simulator_files/example-config.edn


### Output of the simulator

The output of the simulator is a csv of the tasks run. An example output can be found in Cook/scheduler/simulator_files/example-out-trace.csv


## How to analyze the output (coming soon!)

It is important to note that two simulations should only be compared if all inputs were the same (what is being compared is code changes). 

A jupyter notebook is included in Cook/scheduler/simulator_files/analyze.ipynb that has frequent analysis done on a single run of the simulator and analysis for comparing two simulations.


# Developer notes

The simulator sets up the scheduler and a mock of mesos and runs a list of jobs through the scheduler.
For the sake of robustness, it is important to interact with the scheduler as non-invasively as possible so scheduler changes are less likely to break the simulator.

## Bits that deserve explanation:

1. Setting time

To achieve the overarching goal, it is extremely helpful for the simulator to be deterministic.
We have chosen to control and set time to ensure the simulator is deterministic.
This allowed the simulator to interact with the scheduler in only two ways, (a) datomic (b) trigger channels.
Further, it made it possible to use datomic as the source of truth for job statuses.
With regard to the mesos mock, it makes it possible to expire tasks based on time, thus saving us from adding more hooks into the mesos mock.
Without controlling time, we would need to either (a) capture a lot more data (matches, completions, preemptions, max run time exceeded, ..) or (b) store the time of the start and end of the cycle and post hoc map events to the cycles.
In both cases, we would need to add a hook for expiring tasks in the mesos mock that was independent of time.
Both alternatives require more book-keeping and hooks into the scheduler and mesos mock then is desirable.

This relies on the scheduler using clj-time (jodatime) to get time instead of System/currentTimeMillis or java.util.Date.
Thankfully, the code already did that.

Controlling time is notably smelly and the necessity of it should be revisited often.

2. Trigger chans and a word of warning

The trigger channels are a way to control when actions in the scheduler and mesos mock occur.
In normal operation, the scheduler will use chime channels to push an event on a regular interval.
In simulation, the chime channels are replaced with channels the simulator controls in order to trigger processing on demand.
This is very useful but leads to a problem in that new cycle based components will need to follow the same pattern.


## Dev FAQ

1. Why is the time incremented during the cycle?

This was done to aid in debugging so it was possible to tell which step in the cycle something occurred in order to uncover cases where an event occurred at an unexpected point. It is not strictly necessary.
