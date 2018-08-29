<img src="./cook.svg" align="right" width="175px" height="175px">

# Cook Scheduler

[![Build Status](https://travis-ci.org/twosigma/Cook.svg)](https://travis-ci.org/twosigma/Cook)
[![Slack Status](http://cookscheduler.herokuapp.com/badge.svg)](http://cookscheduler.herokuapp.com/)

Welcome to Two Sigma's Cook Scheduler!

What is Cook?

- Cook is a powerful batch scheduler, specifically designed to provide a great user experience when there are more jobs to run than your cluster has capacity for.
- Cook is able to intelligently preempt jobs to ensure that no user ever needs to wait long to get quick answers, while simultaneously helping you to achieve 90%+ utilization for massive workloads.
- Cook has been battle-hardened to automatically recover after dozens of classes of cluster failures.
- Cook can act as a Spark scheduler, and it comes with a REST API and Java client.

[Core concepts](scheduler/docs/concepts.md) is a good place to start to learn more.

## Releases 

Check the [changelog](scheduler/CHANGELOG.md) for release info.

## Subproject Summary

In this repository, you'll find several subprojects, each of which has its own documentation.

* `scheduler` - This is the actual Mesos framework, Cook. It comes with a JSON REST API.
* `jobclient` - This is the Java API for Cook, which uses the REST API under the hood.
* `spark` - This contains the patch to Spark to enable Cook as a backend.

Please visit the `scheduler` subproject first to get started.

## Quickstart

The quickest way to get Mesos and Cook running locally is with [docker](https://www.docker.com/) and [minimesos](https://minimesos.org/). 

1. Install `docker`
1. Clone down this repo
1. `cd scheduler`
1. Run `bin/build-docker-image.sh` to build the Cook scheduler image
1. Run `../travis/minimesos up` to start Mesos and ZooKeeper using minimesos
1. Run `bin/run-docker.sh` to start the Cook scheduler
1. Cook should now be listening locally on port 12321

## Contributing

In order to accept your code contributions, please fill out the appropriate Contributor License Agreement in the `cla` folder and submit it to tsos@twosigma.com.

## Disclaimer

Apache Mesos is a trademark of The Apache Software Foundation. The Apache Software Foundation is not affiliated, endorsed, connected, sponsored or otherwise associated in any way to Two Sigma, Cook, or this website in any manner.

&copy; Two Sigma Open Source, LLC
