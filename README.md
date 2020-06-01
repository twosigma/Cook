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
* `jobclient` - This includes the Java and Python APIs for Cook, both of which use the REST API under the hood.
* `spark` - This contains the patch to Spark to enable Cook as a backend.

Please visit the `scheduler` subproject first to get started.

## Quickstart

### Using Google Kubernetes Engine (GKE)

The quickest way to get Cook running locally against [GKE](https://cloud.google.com/kubernetes-engine) is with [Vagrant](https://www.vagrantup.com/).

1. [Install Vagrant](https://www.vagrantup.com/downloads.html)
1. [Install Virtualbox](https://www.virtualbox.org/wiki/Downloads)
1. Clone down this repo
1. Run `GCP_PROJECT_NAME=<gcp_project_name> vagrant up --provider=virtualbox` to create the dev environment
1. Run `vagrant ssh` to ssh into the dev environment

#### In your Vagrant dev environment

1. Run `gcloud auth login` to login to Google cloud
1. Run `bin/make-gke-test-clusters` to create GKE clusters
1. Run `bin/start-datomic.sh` to start Datomic (Cook database)
1. Run `bin/run-local-kubernetes.sh` to start the Cook scheduler
1. Cook should now be listening locally on port 12321

To test a simple job submission:

1. Run `cs submit --pool k8s-alpha --cpu 0.5 --mem 32 --docker-image gcr.io/google-containers/alpine-with-bash:1.0 ls` to submit a simple job
1. Run `cs show <job_uuid>` to show the status of your job (it should eventually show Success)

To run automated tests:

1. Run `lein test :all-but-benchmark` to run unit tests
1. Run `cd ../integration && pytest -m 'not cli'` to run integration tests
1. Run `cd ../integration && pytest -k test_basic_submit -n 0 -s` to run a particular integration test

### Using Mesos

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
