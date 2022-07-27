
# Main directories in the repository



| Directory   | Description |
| ----------- | ----------- |
| cli   | The cook cli implementation       |
| executor | A replacement for the standard Mesos Executor |
| integration | Integration tests for Cook       |
| jobclient/java | A Java Client API for Cook Scheduler |
| jobclient/python | A Python Client API for Cook Scheduler |
| scheduler | The Cook Scheduler |
| sidecar | The Cook Sidecar. Used on Kubernetes. Responsible for parsing output for progress messages and uploading to Cook. |
| travis | Infrastructure for running unit and integration tests on github actions |



# Setting up a development environment

## Setup a development environment

Setting up a development environment for intellij that understands the various Cook modules involves a few steps. First, 
we need to set up some preliminaries.

### Setup virtualenv's for the python code

You need to at least setup one virtualenv for running integration tests. If you want to develop other python modules in 
the Cook repo, I recommend setting up all the virtualenv's. In addition, Intellij needs a Python SDK for creating projects 
for Python modules, so we need to make appropriate Python environments.

For each of the python repositories (cli, executor, jobclient/python, integration, sidecar) , you should set up a 
virtualenv for development. Python 3.9 has been tested.

Within each directory, I make a virtualenv:
```
(cd cli ; python3 -m virtualenv virtualenv-cli)
(cd executor ; python3 -m virtualenv virtualenv-executor)
(cd integration ; python3 -m virtualenv virtualenv-integration)
(cd jobclient/python ; python3 -m virtualenv virtualenv-jobclient)
(cd sidecar ; python3 -m virtualenv virtualenv-sidecar)
```

If you're not familiar with virtualenv's, when activated they set up a special shell environment that's an isolated 
Python environment. To use, cd into the various child directories and activate it, giving an appropriate shell.

```source virtualenv-integration/bin/activate```

Then within the virtualenv, you cause Python to set up its Python environment with all appropriate dependencies. This 
step can also be repeated when dependencies change. 

In `sidecar`, `executor`, `cli`, activate the virtualenv in a new shell window and do, 

```python setup.py install```

In `integration` and `python/jobclient` activate the env in a new shell window and do:

```pip3 install -r requirements.txt ```

Later on if you want to use the environments for development, activate to get an appropriate shell and then just use it. 
E.g., for integration tests, I do: 

```
source virtualenv-integration/bin/activate
pytest ....
```

### Setting up intellij

Cook is developed in intellij.

The recommended project setup makes it easy to edit cook with its various clojure, python, and java modules. What we 
want to do is make individual modules for the various pieces of Cook and a module 'root' that contains all of the files 
that are not contained in one of those submodules. Because modules cannot overlap, we create a root module, and may need 
to exclude the submodules (intellij behavior changes and seems to be inconsistent on whether exclusion is needed or not).

1. Create a new project at the toplevel, with a module 'root' that includes the root of the GitHub checkout.
1. Within that module, you may need to mark the following directories as excluded. This allows us to make modules for them 
   in the next step. (right click -> Mark Directory -> Excluded): sidecar, scheduler, cli, integration, executor, 
   jobclient/python jobclient/java, simulator.

The next step is I made new python SDK's based on the existing virtualenv's. (You only need to do this step for the submodules 
you develop; I only did for `integration tests`, `jobclient/python`, and `cli`)

1. File -> Project Structure -> SDK's. 

Now add modules to the project for the 8 directories for submodules. You may or may need to exclude them from the 'root' project:

1. File -> New -> Module from existing sources

* For the python modules, make sure to use the SDK's you created above.
* For `jobclient/java`, chose import form an external model in the wizard and choose maven.
* For `cook-scheduler`, it will ask which model to use, Choose Leiningen, then `project.clj`.

I tend to rename the module names. E.g., `cook-scheduler`.

Now you should have an IDE where you can click around to find definitions and uses, in particular for Cook scheduler 
as well as the integration tests.

You want to teach Cursive about the main Cook macros. See https://cursive-ide.com/userguide/macros.html

In particular, `mount.core/defstate` should map to `def`.

### Downloading datomic

* Download the datomic jar datomic-free-0.9.5561.56.zip and put it in scheduler/datomic from https://clojars.org/com.datomic/datomic-free/versions/0.9.5561.56
* Unzip this to get the underlying jar

### Getting a REPL running

A REPL lets you interactively run tests within your IDE as well as test code.

1. Download datomic (see above) and unzip. 
1. Add the datomic-free-0.9.5561.56.jar as a library to the cook-scheduler module.
1. Add datomic/datomic-free-0.9.5561.56/lib as a directory of jars. 

You need to setup a REPL configuration:

* Make a new REPL. 'Run -> Edit Configurations -> + -> Clojure REPL' I call mine 'Cook REPL'.
* Set it to be nrepl type and use Leiningen. Set profiles to be `+oss,+test` 
  * Clojure lets you change your dependencies based on preconfigured profiles. Profiles are defined in `project.clj` 
    or a parallel `profiles.clj` file.
  * +oss, sets the right dependencies for running opensource, and +test activates the profiles.
  * Scroll down and ensure the repl runs with the environmental variable `COOK_DB_TEST_PG_SCHEMA` set to anything.

Note that you may get a path resolution issue about relative paths in modern lein. This diff includes a workaround for it.

https://github.com/cursive-ide/cursive/issues/2624

# Using your dev environment on Cook Scheduler

These operations assume you're in the `scheduler` directory, or, in the IDE, are in the cook-scheduler module.

## Using a REPL

Launch your REPL.

If you are in a clojure file and right click, you can choose to send the file or the current clause to the REPL. 

## To unit test cook scheduler

### Initial setup

Make sure you have a more recent lein. Version 2.9.1 is known to work. Older versions are known to improperly compute dependencies.

```
lein version
```
Download any dependencies:
```
lein deps 
```

### Compile

```
lein compile
```

### Unit tests

To run unit tests:

```
export COOK_DB_TEST_PG_SCHEMA=cook_local && time lein test
```

This export is an artifact of postgresql support and must be set so that the test infrastructure doesn't try to create 
a database. As we do not use postgresql at the current time, there's no need to set up a database.

You can also run only some tests:

```
lein test :only cook.test.quota/test-quota
lein test :only cook.test.quota
```

### Unit tests in the IDE

Unit tests in the IDE can be a convenient way to get a faster edit-compile-test cycle as reusing an existing repl is much faster than launching a new clojure environment.

1. Launch a REPL
1. In a unit test file, right click, and you should see options to run a single test in the REPL, or run the entire 
   namespace (i.e., file) in the REPL.
1. Note that if you change the code, you need to reload changes in the REPL for it to reflect it. I have an IDE script 
   that saves all files, syncs all files to the REPL, removes all test markers, and reruns the last test namespace in 
   the REPL. (See options when right-clicking in the REPL window or in the code window)

### Diagnosing unit test errors

If you get an error about `mount.core.DerefableState`, such as:
```
actual: java.lang.ClassCastException: class mount.core.DerefableState cannot be cast to class com.google.common.cache.Cache
```

when running a unit test or file, that is because many unit tests don't run `(testutil/setup)` to initialize some singletons on 
startup. (These are managed by `mount`), assuming other unit tests have already have done a setup. Just add a `(testutil/setup)`
at the top of a test and fix them as you discover them.

## Running integration tests (the easy way)

## Create your GKE clusters

You need a GKE project configured and gcloud locally installed so that you can use it to create GCP resources.

Then, to create the cluster, specifying your GCP project, a region (I suggest central), and a name prefix to use for the clusters.

```
bin/make-gke-test-clusters <<<project>>> us-central1-b test-cluster-5
```

Note that this is intended for dev projects only and will **delete existing clusters**. (If you want to abandon existing 
clusters, make ones with new names.)

## Start cook

After your clusters have been created, we need to start cook. I do a compile just to make sure I don't have a compile 
error. Otherwise the build may fail and docker runs an old build.

```
lein compile && bin/build-docker-image.sh && export COOK_CONFIG='config-k8s.edn' && bin/run-docker.sh
```

Cook's logs will show up in `log/*`.


## Run integration tests

Then in another shell, go into the integration test directory and activate the virtualenv:

```asciidoc
source virtualenv-integrationtest/bin/activate
```

Inside of that virtualenv, I have historically run:

```asciidoc
export COOK_DEFAULT_JOB_CPUS=0.01 COOK_DEFAULT_JOB_MEM_MB=32 COOK_TEST_DOCKER_IMAGE=python:3.5.9-stretch COOK_TEST_DEFAULT_SUBMIT_POOL=k8s-gamma COOK_TEST_COMPUTE_CLUSTER_TYPE=kubernetes COOK_TEST_DISALLOW_POOLS_REGEX='(?!k8s.*)' ; time pytest -n6 -v --timeout-method=thread --boxed -m "not memlimit and not cli and not scheduler_not_in_docker" --log-level=ERROR tests/cook/test_basic.py  
```

This should give you a clean run of the integration tests.

### Running all integration tests

For unfortunate reasons, the full test suite has not been run by anyone in opensource in a long time and has bitrotten. 
Only the set above was maintained to be green in OSS. To run everythingin OSS:

```asciidoc
export COOK_DEFAULT_JOB_CPUS=0.01 COOK_DEFAULT_JOB_MEM_MB=32 COOK_TEST_DOCKER_IMAGE=python:3.9 COOK_TEST_DEFAULT_SUBMIT_POOL=k8s-gamma COOK_TEST_COMPUTE_CLUSTER_TYPE=kubernetes COOK_TEST_DISALLOW_POOLS_REGEX='(?!k8s.*)' ; time pytest -n8 -v --timeout-method=thread --boxed -m "not memlimit and cli and not scheduler_not_in_docker" --log-level=ERROR --maxfail 20
```

**Do not expect a clean run**

## Running integration tests (the manual way)

1. Start datomic manually.
1. Initialize pools manually.
1. Start cook outside of docker.
1. Run the pytests.

## Ad-hoc clojure testing and development 

Clojure has a REPL and IntelliJ lets you make scratches. It's perfectly reasonable to use that functionality to 
development ane explore clojure outside of Cook. 
This can be particularly helpful if we keep our functions pure --- not accessing external state. That way they can be 
developed independently of COok and unit tested standalone.


# Vagrant

Vagrant support was added, but its functionality and design is not well understood by the present team. 



