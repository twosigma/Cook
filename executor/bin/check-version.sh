#!/bin/bash

# USAGE: ./bin/check-version.sh [-q|--quiet]
#
# Returns 0 (true) if the currently-installed cook executor version
# matches the source version, and 1 (false) otherwise.
#
# When using the quiet flag (-q|--quiet), no additional output is printed.
#
# Note: versions containing the case-insensitive string "SNAPSHOT" never match
# (i.e., they're considered "unstable", and should always require a rebuild).

#
# Parse options
#

case "$1" in
    -q|--quiet)
        shift
        verbose=false
        ;;
    '')
        verbose=true
        ;;
    *)
        echo "USAGE: $0 [-q|--quiet]"
        exit -1
esac

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
# Check for source code SNAPSHOT version
#

shopt -s nocasematch

if [[ "$source_version" == *SNAPSHOT* ]]; then
    if $verbose; then
        echo "Detected snapshot (unstable) version: $source_version"
    fi
    exit 1
fi

#
# Get installed binary's version
#

binary_app=./dist/cook-executor

if [ -e $binary_app ]; then
    installed_version="$($binary_app --version)"
else
    installed_version=none
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
