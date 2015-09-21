# Cook Scheduler

[![Build Status](https://travis-ci.org/twosigma/Cook.svg)](https://travis-ci.org/twosigma/Cook)

Welcome to Two Sigma's Cook Scheduler!

You'd probably like to run Spark jobs on Cook, right?
First, go to the `scheduler` subproject, and follow the README to build and launch the Cook scheduler Mesos framework.
Then, go to the `spark` subproject, and follow the README to patch Spark to support Cook as a scheduler.
If you'd like to learn more or do something different, read on...

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
