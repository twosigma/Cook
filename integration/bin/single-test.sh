#!/bin/bash

# Usage: ./bin/single-test.sh TEST
# Locates the specified TEST method name,
# then builds, runs and times the nosetests command for that single test.
# Sample usage: ./bin/single-test.sh test_basic_submit

if [ -z "$1" ]; then
    echo "Must provide test name argument" >&2
    exit 1
fi

test_name="$1"
test_file="$(git grep -l "^ *def $test_name(" -- tests/cook/)"
test_class="$(sed -n 's/^class \([_[:alnum:]]*\)(unittest.TestCase):.*/\1/p' < "$test_file")"

if [ -z "$test_file" ]; then
    echo "Test not found: $test_name" >&2
    exit 1
elif [ -z "$test_class" ]; then
    echo "Test class not found for $test_name in $test_file" >&2
    exit 1
fi

echo "Found $test_name in $test_file:$test_class"

export COOK_MULTI_CLUSTER=
time nosetests --processes=0 --verbosity 3 --nologcapture --tests "$test_file:$test_class.$test_name"
