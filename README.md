# Cook Scheduler

[![Build Status](https://travis-ci.org/twosigma/Cook.svg)](https://travis-ci.org/twosigma/Cook)

Welcome to Two Sigma's Cook Scheduler!

What is Cook?

- Cook is a powerful batch scheduler, specifically designed to provide a great user experience when there are more jobs to run than your cluster has capacity for.
- Cook is able to intelligently preempt jobs to ensure that no user ever needs to wait long to get quick answers, while simultaneously helping you to achieve 90%+ utilization for massive workloads.
- Cook has been battle-hardened to automatically recover after dozens of classes of cluster failures.
- Cook can act as a Spark scheduler, and it comes with a REST API and Java client.

But you'd probably like to run Spark jobs on Cook, right?
To do so, download the latest Cook scheduler [here](https://github.com/twosigma/Cook/releases).
You can launch the scheduler for testing by running `java -jar cook-release.jar dev-config.edn` (get `dev-config.edn` [here](https://github.com/twosigma/Cook/blob/master/scheduler/dev-config.edn); read more about configuration in `scheduler/docs/configuration.asc`).
Then, go to the `spark` subproject, and follow the README to patch Spark to support Cook as a scheduler.
If you'd like to learn more or do something different, read on...

## Releases 

Check the [changelog](CHANGELOG.md) for release info.

## Subproject Summary

In this repository, you'll find several subprojects, each of which has its own documentation.

* `scheduler` - This is the actual Mesos framework, Cook. It comes with a JSON REST API.
* `jobclient` - This is the Java API for Cook, which uses the REST API under the hood.
* `spark` - This contains the patch to Spark to enable Cook as a backend.

Please visit the `scheduler` subproject first to get started.

## Quickstart

The quickest way to get Mesos and Cook running locally is with [docker](https://www.docker.com/) and [minimesos](https://minimesos.org/). 

1. Install `docker`
2. Install `minimesos`
3. Clone down this repo
4. `cd scheduler`
5. Run `docker build -t cook-scheduler .` to build the Cook scheduler image
6. Run `minimesos up` to start Mesos and ZooKeeper
7. Run `bin/run-docker.sh` to start the Cook scheduler
8. Cook should now be listening locally on port 12321

## Contributing

In order to accept your code contributions, please fill out the appropriate Contributor License Agreement in the `cla` folder and submit it to tsos@twosigma.com.

## Disclaimer

Apache Mesos is a trademark of The Apache Software Foundation. The Apache Software Foundation is not affiliated, endorsed, connected, sponsored or otherwise associated in any way to Two Sigma, Cook, or this website in any manner.

© Two Sigma Open Source, LLC
