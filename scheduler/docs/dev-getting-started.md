# Setting up your Cook dev environment

This document tells you how to set up a Cook dev environment from
scratch. We have to install Clojure itself, Datomic, Docker, and Mesos.

Prerequisites
=============

Before beginning, you should already have working installations of Clojure and [Leiningen](https://leiningen.org/).
Refer to those projects' getting started guides for information on how to set
them up.


Installing Cook-specific Infrastructure
========================================


Docker
-----

Install docker by following the instructions on:

https://docs.docker.com/engine/installation/linux/ubuntulinux/

There is install docs for all common OSs.

Minimesos
-----

Then, install minimesos by following the instructions at
http://minimesos.readthedocs.io/en/latest/ .


Once minimesos is installed, you can download and run minimesos itself:


```
mkdir minimesos
cd minimesos

minimesos init
minimesos up --num-agents 2
```

Big Mesos
---------

Even if you are using minimesos, you still have to build regular Mesos
to get the `libmesos.so` library (called `libmesos.dylib` on Mac).

You can either install it directly on your machine or use a docker container with 
mesos installed. Here we will only talk about using a docker container. 
If you instead want to install mesos on your machine, you can follow the docs here:
http://mesos.apache.org/gettingstarted/

The following repo contains a DockerFile that will set up Cook,
use this as a starting point to get set up:
https://github.com/wyegelwel/cook-docker


Command Line Usage
==================

To build and run the project at the command line, copy
`$COOK_DIR/scheduler/dev-config.edn` and edit the copy so that the Mesos master ZooKeeper URL matches
the one returned by `minimesos info`.

Then run the following, replacing `$MESOS_DIR` with the actual path to your local
Mesos build:


```
cd $COOK_DIR/scheduler
lein uberjar
MESOS_NATIVE_JAVA_LIBRARY=$MESOS_DIR/build/src/.libs/libmesos.so lein run ./local-dev-config.edn
```

Test that the server is running properly with:

```
curl http://localhost:12321/rawscheduler
```

If you get a reply like `"must supply at least one job query param"`, that means Cook is running.


Interactive development
=======================

The dev config will open a nrepl port on the running cook server. 
You can connect to this port and then develop, eval and test on the running server. 
We have found this greatly speeds up development and is just generally pleasant. 
