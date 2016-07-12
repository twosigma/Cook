# Setting up your Cook dev environment

This document tells you how to set up a Cook dev environment from
scratch on a Macintosh computer.

Prerequisites
=============

Before beginning, you should already have working installations of MacPorts,
Clojure, Leiningen, and Emacs (or your favorite text editor). Refer to
those projects' getting started guides for information on how to set
them up.

You will need the latest Xcode and Xcode Command Line Tools packages from
Apple. The Command Line Tools can no longer be installed through
XCode. To get them, log in at https://developer.apple.com/, go to
"Downloads," then scroll to click on "See more downloads" at the
bottom. From there, download and install the "Command Line Tools"
package that corresponds to your version of Xcode.

This guide assumes you're using MacPorts. Homebrew will work
similarly, though you may have to adjust a few paths to correspond to
those that Homebrew uses.


Installing Cook-specific Infrastructure
========================================

We need to install Datomic, Docker, and Mesos.


Datomic
-------

Create an account at www.datomic.com. Download the "Datomic Pro
Starter" package and unzip it somewhere.

Install it into your local Maven repository with:

```
./bin/maven-install
```


Docker
-----

Install Docker from https://www.docker.com/products/docker#/mac .


Vagrant
-------
Install Vagrant from https://www.vagrantup.com/downloads.html .




Minimesos
-----

If you're using the Docker native Mac app beta, you can't use
minimesos until 0.9.1 is released (scheduled for sometime in July
2016.) Minimesos <= 0.9.0 has a bug that prevents it from working on
the Docker 1.12 that comes with the Docker for Mac app.

In the meantime, you can get Docker 1.11 on a Mac by installing the
older "Docker Toolbox" app from
https://www.docker.com/products/docker-toolbox .

Then, install minimesos by following the instructions at
http://minimesos.readthedocs.io/en/latest/ .


Once minimesos is installed, you can download and run minimesos itself:


```
mkdir minimesos
cd minimesos

minimesos init
minimesos up --num-agents 2
```


Finally, tell your networking stack to route packets for the Docker
172.17.0.0/16 network to the virtual network interface exposed by the
VM.

```
sudo route delete 172.17.0.0/16; sudo route -n add 172.17.0.0/16 $(docker-machine ip ${DOCKER_MACHINE_NAME})
```

You can verify that everything is working by doing `minimesos info`, then
pinging the IP addresses returned and attempting to connect to the HTTP
based services with curl or a web browser.



Big Mesos
---------

Even if you are using minimesos, you still have to build regular Mesos
to get the `libmesos.dylib` library (called `libmesos.so` on Linux).


```

sudo port install wget git autoconf automake libtool subversion
#
# Or 'brew install ...' if you use homebrew instead of MacPorts.
#
# This will take a while if you don't already have these installed.


git clone https://git-wip-us.apache.org/repos/asf/mesos.git
cd mesos

./bootstrap

mkdir build && cd build

# PYTHON= is a workaround for the Python bug
# described at http://gwikis.blogspot.com/2015/08/building-mesos-0230-on-os-x-yosemite.html
#
# The --with-svn=/opt/local/ flag makes it use the MacPorts copy of Subversion rather
# than the system copy, which does not include the dev headers and library.
#

PYTHON=/usr/bin/python ../configure --with-svn=/opt/local/

# Add MacPorts libraries to the linker search path and build.
# This build takes a while...
LIBRARY_PATH=/opt/local/lib make

```


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
MESOS_NATIVE_JAVA_LIBRARY=$MESOS_DIR/build/src/.libs/libmesos.dylib lein run ./local-dev-config.edn
```

Test that the server is running properly with:

```
curl http://localhost:12321/rawscheduler
```

If you get a reply like `"must supply at least one job query param"`, that means Cook is running.


Emacs interactive startup
===========================

To start Cook interactively from within Emacs, first make sure you
have Cider (the Clojure nrepl package) installed and working.

Do `M-x setenv` and set `MESOS_NATIVE_JAVA_LIBRARY` to the full path
to `libmesos.dylib`, as above (or otherwise make sure this env var is
set for your Emacs process.) This must be done *before* you start Cider.

Load `$COOK_DIR/scheduler/src/cook/components.clj` and jack in (`C-c M-j`) as usual.

Once loaded, run:

```
cook.components> (-main "$COOK_DIR/scheduler/local-dev-config.edn")
```



