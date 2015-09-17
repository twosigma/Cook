# Cook Scheduler

Welcome to Two Sigma's Cook Scheduler!

You'd probably like to run Spark jobs on Cook, right?
First, go to the `cook` subproject, and follow the README to build and launch the Cook scheduler Mesos framework.
Then, go to the `spark` subproject, and follow the README to patch Spark to support Cook as a scheduler.
If you'd like to learn more or do something different, read on...

## Subproject Summary

In this repository, you'll find several subprojects, each of which has its own documentation.

* `scheduler` - This is the actual Mesos framework, Cook. It comes with a JSON REST API.
* `jobclient` - This is the Java API for Cook, which uses the REST API under the hood.
* `spark` - This contains the patch to Spark to enable Cook as a backend.
* `datomic-migration` - This tool allows you to clean old data from your Cook database. This tool should be run every 6-9 months to keep Cook fast.
* `agent` - This contains an example enhanced Mesos executor for Cook which supports progress updates and heartbeats.

Please visit the `scheduler` subproject first to get started.

## Contributing

In order to accept your code contributions, please fill out the appropriate Contributor License Agreement in the `cla` folder and submit it to tsos@twosigma.com.

## Disclaimer

Apache Mesos is a trademark of The Apache Software Foundation. The Apache Software Foundation is not affiliated, endorsed, connected, sponsored or otherwise associated in any way to Two Sigma, Cook, or this website in any manner.

© Two Sigma Open Source, LLC
