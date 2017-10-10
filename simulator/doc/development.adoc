# Cook Simulator Development


## System Components

The top-level components of Cook Simulator include:

* The Simulator code itself, which defines how to run simulations and interpret their results.
* The Simulator's own Datomic database keeps track of intended work schedules,
past simulation runs, etc.
* Cook Scheduler's Datomic database (presumably a separate database), knowledge from which is combined with the Simulator Database to formulate an understanding of how jobs in simulations actually performed.
* Cook Scheduler's API, which Simulator uses to schedule jobs during a simulation run.

## Important Clojure Libraries

Cook Simulator builds on functions provide by several libraries.  If you're going to be developing on the Simulator, it makes sense to begin by reading all of the top-level documenation for each of these libraries.

* To access the system components, Simulator relies on [Stuart Sierra's "Component"](https://github.com/stuartsierra/component).  Component facilitates explicit management of global state at runtime, and makes REPL development easier in various ways (e.g. it provides a mechanism to consistently reload clojure code).
* [Simulant](https://github.com/Datomic/simulant) is the basis for the way that Cook Simulations are modelled and executed.  A Simulator "Schedule" (or "Workload") an example of a Simulant "Test"; while a "Simulation" is an example of a simulant "sim".
* [Incanter](https://github.com/incanter/incanter) is used to generate the visual reports.


## The REPL

If you are going to be working with Cook Simulator a lot, or even extending it,
you're going to want to spend most of your time at the REPL rather than at the
command line interface.

[The cook.sim.repl namespace](../src/dev/cook/sim/repl.clj) exists for the sole purpose of making development easier.  When you launch a REPL, this is the namespace you'll arrive at by default.  repl.clj is in the "src/dev" source path rather than the "src/main"
source path, and you can see in [project.clj](../project.clj) that it's not even included in the default lein profile (so, for example, it won't be included in a distributable jar by default).  There's a separate profile, "dev" which includes cook.sim.repl.

cook.sim.repl imports all of the main namespaces of the project, and provides
convenience functions to facilitate doing a lot of simulation-related work quickly.
The intent is that you should be able to leave a REPL open indefinitely, and never
switch to another namespace; you can call whatever functions you want to test/use
from your home in cook.sim.repl.

The convenenience functions in cook.sim.repl do make some assumptions about things like
the locations of your configuration files and your workload files.  cook.sim.repl in general is designed more for convenience than for flexibility.

Before you load the REPL, make sure that your config/settings.edn points to the correct locations for the various system components:  Simulator Database, Cook (scheduler) Database, and Cook Scheduler API.

When you first load the REPL, you will need to call (reset()) in order to actually start up all of the system components so that they can be used (among other things, this will establish a connection to both databases).  (reset()) can also be called later on to reload all clojure code and return the REPL to a known runtime state.


## Reporting

Currently, Cook Simulator supports three ways to analyze the results of simulations.
In general, code that relates to this sort of presentation  will be in the "cook.sim.reporting" namespace.

### Text output

cook.sim.reporting/analyze  is a function that will print out a summary of how the jobs in a single simulation performed - average wait time, time-to-finish, and "Overhead" (time-to-finish minus job runtime), as well as more information about any jobs that never finished at all.

### Job-by-job comparison charts

cook.sim.reporting/job-by-job comparison-chart will generate a chart that compares performance percentiles (according to a single metric) between multiple runs of a single workload.  This could be used to evaluate the immediate effect of a given change to Cook Scheduler:  Using the same cluster configuration and workload, run a sim with each branch of Scheduler, and compare the performance percentiles of wait-time, overhead, etc.

### "Knob turning" charts

This type of chart is only accessible via the REPL.  It is designed to help to understand the effect of changing a Cook Scheduler setting across a range of values.  See cook.sim.reporting/knob-turning-chart for details.

## Strategic Notes

### Evaluating changes to Cook Scheduler

One of the most important ways that the Simulator can be used is to test the effect of changes to Cook Scheduler.  To compare directly, you'll want to be using the same version of Simulator to run simulations against multiple versions of Scheduler.

Since Scheduler is (at least for the time being) in the same repository as Simulator, this likely means that if you plan on testing changes or extensions to the Simulator while simultaneously comparing performance across different versions of Scheduler, you'll want to check out two local copies of the Cook repository, one to run Simulator, and one to run Scheduler.


### Simulating a Larger Cluster

The jobs generated by Cook Simulator require allocation of resources from Mesos, but they don't actually consume any resources.  For this reason, you can simulate a very large
Mesos cluster without needing to actually pay for a large Mesos cluster.

Simply start any number of Mesos slave procesess (containers or local processes) and configure each of them to advertise e.g. 30 CPU's and 600 GB of memory.  Unless your tasks actually consume the cpu's or memory (which Cook Simulator's tasks won't), the "bluff" will never be called.

### One Sim at a time

Currently, when generating reports, Cook Simulator expects to connect to only a single
Cook Scheduler database.  Thus, it's necessary that, for a given set of Simulations
to be compared, all were run against the same installation of Cook Scheduler.

This has a ramification which certainly increases the amount of time it takes to run a significant series of Simulations (e.g. for a "Knob Turning Chart").  Since it's not possible for one installation of Cook Scheduler to run multiple simulations simultaneously (the jobs from different Sims would compete with each other for the cluster's resources), it's only possible to run one Simulation in a series at a time.


### Sharing workloads

Cook Simulator stores all of the data it uses in Datomic, but in the course of generating a workload, the details about all of the jobs in the workload also live in a file on the filesystem.  The reason for this is that it makes it easy to examine, share, and edit specific workloads.

For example, you might have a job schedule that demonstrates a flaw in a new branch of cook.  Since the schedule is represented in a physical file, you can easily mail that file to a developer.  He can then look at it, edit the file to zero in on the problem, use a file diffing tool to communicate the differences, send an adjusted version back to you, and so forth.


## Continuous Integration

The "cook.sim.travis" namespace is specifically dedicated to running simulations on Travis CI.  The idea is that on every CI run, a single simulation will be invoked.  The sim will run against a cluster cluster configuration known to be capable of completing all  of schedulable jobs in the simulation within a specific amount of time.  The build will fail if the interval elapses and any of the schedulable jobs haven't been finished.

Unschedulable jobs are also included in the workload, to make sure that Cook still
behaves even when it can't schedule certain jobs.

Non-clojure files that relate to Travis CI are present in the top-level "travis/" directory.
