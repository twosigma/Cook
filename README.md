# Cook Scheduler

[![Build Status](https://travis-ci.org/twosigma/Cook.svg)](https://travis-ci.org/twosigma/Cook)

Welcome to Two Sigma's Cook Scheduler!

What is Cook?

- Cook is a powerful batch scheduler, specifically designed to provide a great user experience when there are more jobs to run than your cluster has capacity for.
- Cook is able to intelligently preempt jobs to ensure that no user ever needs to wait long to get quick answers, while simultaneously helping you to achieve 90%+ utilization for massive workloads.
- Cook has been battle-hardened to automatically recover after dozens of classes of cluster failures.
- Cook can act as a Spark scheduler, and it comes with a REST API and Java client.

[Core concepts](scheduler/docs/concepts.md) is a good place to start to learn more.

## Subproject Summary

In this repository, you'll find several subprojects, each of which has its own documentation.

* `scheduler` - This is the actual Mesos framework, Cook. It comes with a JSON REST API.
* `jobclient` - This is the Java API for Cook, which uses the REST API under the hood.
* `spark` - This contains the patch to Spark to enable Cook as a backend.

Please visit the `scheduler` subproject first to get started.

## Contributing

In order to accept your code contributions, please fill out the appropriate Contributor License Agreement in the `cla` folder and submit it to tsos@twosigma.com.

## Disclaimer

Apache Mesos is a trademark of The Apache Software Foundation. The Apache Software Foundation is not affiliated, endorsed, connected, sponsored or otherwise associated in any way to Two Sigma, Cook, or this website in any manner.

© Two Sigma Open Source, LLC
