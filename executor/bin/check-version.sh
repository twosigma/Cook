#!/bin/bash

# USAGE: ./bin/check-version.sh [-q|--quiet] [EXECUTOR_NAME]
#
# Returns 0 (true) if the currently-installed cook executor version
# matches the source version, and 1 (false) otherwise.
#
# When using the quiet flag (-q|--quiet), no additional output is printed.
# You can override the default binary name (cook-executor) using
# the optional EXECUTOR_NAME argument.
#
# Note: versions containing the case-insensitive string "dev" never match
# (i.e., they're considered "unstable", and should always require a rebuild).

#
# Parse options
#

if [[ "$1" =~ ^(-q|--quiet)$ ]]; then
    shift
    verbose=false
else
    verbose=true
fi

if [[ $# > 1 ]]; then
    echo "USAGE: $0 [-q|--quiet] [EXECUTOR_NAME]"
    exit -1
fi

EXECUTOR_NAME="${1:-cook-executor}"

#
# Switch to executor directory
#

executor_bin_dir="$(dirname ${BASH_SOURCE[0]})"

cd "$executor_bin_dir/.."

#
# Get source code version
#

source_version=`python3 -c 'from cook._version import __version__; print(__version__)'`

#
# Check for source code dev version
#

shopt -s nocasematch

if [[ "$source_version" == *dev* ]]; then
    if $verbose; then
        echo "Detected dev (unstable) version: $source_version"
    fi
    exit 1
fi

#
# Get installed binary's version
#

binary_app=./dist/${EXECUTOR_NAME}/${EXECUTOR_NAME}

host-libc-matches-container-libc() {
    # Check if it's safe to locally run the pyinstaller binaries from docker.
    # To get the expected libc version: ldd $binary_app | awk '/libc/ { print $3 }'
    libc_path_generic='/lib64/libc.so.6'
    libc_path_multiarch='/lib/x86_64-linux-gnu/libc.so.6'
    [[ "$(uname -s -p)" == 'Linux x86_64' ]] && [[ -e $libc_path_generic || -e $libc_path_multiarch ]]
}

if ! [ -e "$binary_app" ]; then
    # Executor binary not found
    installed_version=none
elif host-libc-matches-container-libc; then
    # We can run the executor directly on x64 Linux (same as docker platform)
    installed_version="$(${binary_app} --version)"
else
    # Run the executor directly on all other platforms
    installed_version="$(docker run --rm -v $(pwd):/opt/cook python:3.5.9-stretch /opt/cook/${binary_app} --version)"
fi

#
# Compare source vs installed binary versions
#

if [ "$installed_version" == "$source_version" ]; then
    if $verbose; then
        echo "At most recent version: $installed_version"
    fi
    exit 0
else
    if $verbose; then
        echo "Version mismatch: $installed_version installed vs $source_version available"
    fi
    exit 1
fi
