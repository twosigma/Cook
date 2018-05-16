#!/bin/bash

PACKAGE_CACHE_DIR=$HOME/.apt-cache

if [ -d "$PACKAGE_CACHE_DIR" ] && [ -n "$(find $PACKAGE_CACHE_DIR -name 'mesos_*.deb')" ]; then
    echo 'Using cached Mesos library...'
    cp -f $PACKAGE_CACHE_DIR/*.deb /var/cache/apt/archives/
else
    echo 'Downloading Mesos library...'
    apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF
    echo "deb https://repos.mesosphere.io/ubuntu/ trusty main" | sudo tee /etc/apt/sources.list.d/mesosphere.list
    apt-get update -qq
    apt-get install mesos -y --download-only
    mkdir -p $PACKAGE_CACHE_DIR/
    cp -f /var/cache/apt/archives/*.deb $PACKAGE_CACHE_DIR/
fi

set -x

apt-get install --allow-downgrades --fix-broken --no-download --yes $PACKAGE_CACHE_DIR/*.deb
APT_EXIT_CODE=$?

if [ $APT_EXIT_CODE -ne 0 ] || ! [ -f $MESOS_NATIVE_JAVA_LIBRARY ]; then
    echo 'Mesos installation error!'
    exit $APT_EXIT_CODE
fi
