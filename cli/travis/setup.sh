#!/bin/bash

cli_dir="$(dirname "$( dirname "${BASH_SOURCE[0]}" )" )"
cd "$cli_dir"

# Don't use --user in virtualenv
if [[ "$(pip -V)" != *${HOME}* ]]; then
    pip_flags=--user
else
    pip_flags=
fi

# Parse dependencies from setup.py
dependencies="$(sed -nE "s/^\\s+'([^']+)',\$/\\1/p" < setup.py)"

pip install $pip_flags $dependencies
