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

# Add MacPorts libraries to the linker search path:
LIBRARY_PATH=/opt/local/lib make


```


Command Line Usage
==================

1. Start
