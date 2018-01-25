Cook Executor
=============

The Cook executor is a custom executor written in Python.
It replaces the default command executor in order to enable a number of features for both operators and end users.
For more information on Mesos executors, see the "Working with Executors" section at http://mesos.apache.org/documentation/latest/app-framework-development-guide/

For Users
---------

The cook executor supports a number of features beyond the standard Mesos command executor.
The executor sends the following information to the scheduler:
1. progress update messages from the task (at intervals of `progress-sample-interval-ms` ms, see below),
2. the exit code of the task, and
3. the sandbox location of the task.

For Operators
---------

The Cook executor replaces the default command executor.
It can either be installed on each agent, or downloaded on each agent via a `uri`.
Because the executor communicates with Mesos agent via HTTP, the agent must support the HTTP API and have it enabled.

### Installation

First, make sure `python3` is installed on each agent.

Then, to install the executor executable (`cook-executor`) and all dependencies, run:

```bash
$ python setup.py install
```

Alternatively, [`pyinstaller`](https://github.com/pyinstaller/pyinstaller) can be used to build a single executable that is distributed via a URI.
For example, to build the executable, run the following command from the `executor` folder:

```bash
# will create dist/cook-executor
$ pyinstaller -F -n cook-executor -p cook cook/__main__.py
```

For development, a version of the cook executor can be built using:

```bash
# will create dist/cook-executor-local
$ bin/build-local.sh
INFO: PyInstaller: 3.3
INFO: Python: 3.5.3
...
INFO: Appending archive to EXE .../executor/dist/cook-executor-local
INFO: Fixing EXE for code signing .../executor/dist/cook-executor-local
INFO: Building EXE from out00-EXE.toc completed successfully.
```

Also, a docker-compatible version of the cook executor can be built using:

```bash
# will create dist/cook-executor
$ bin/build-docker.sh
...
INFO: PyInstaller: 3.3
INFO: Python: 3.5.4
...
INFO: Appending archive to ELF section in EXE /opt/cook/dist/cook-executor
INFO: Building EXE from out00-EXE.toc completed successfully.
```

### Configuration

Configuration for the executor should go under the `:executor` key of the edn configuration file for the cook scheduler.

For example:

```clojure
{...
 :executor {:command "mkdir .cook-executor && cook-executor"
            :default-progress-output-file "stdout"
            :default-progress-regex-string "progress: ([0-9]*\.?[0-9]+), (.*)"
            :log-level "INFO"
            :max-message-length 512
            :progress-sample-interval-ms 1000
            :uri {:cache true
                  :executable true
                  :extract false
                  :value "file:///path/to/cook-executor"}}
 ...}
```

The command needs to create the `.cook-executor` directory (where we extract libraries and support files) before launching the cook executor.
For more detailed information about the executor configuration, see the Cook Executor section in [configuration documentation](../scheduler/docs/configuration.adoc).
Some of the executor configurations are sent to the Cook executor as the following environment variables:

| environment variable name | executor config key |
|---------------------------|---------------------|
| `EXECUTOR_LOG_LEVEL` | `:log-level` |
| `EXECUTOR_MAX_MESSAGE_LENGTH` | `:max-message-length` |
| `PROGRESS_OUTPUT_FILE` | `:default-progress-output-file` |
| `PROGRESS_REGEX_STRING` | `:default-progress-regex-string` |
| `PROGRESS_SAMPLE_INTERVAL_MS` | `:progress-sample-interval-ms` |

### Tests

The cook executor uses `pytest`.
To install test dependencies and run the executor test suite, run:

```bash
$ pytest
```

If you want to run a single test and see the log messages as they occur, you can run, for example:

```bash
$ pytest -svk test_get_task_id
```

### Troubleshooting

If the executor is not correctly installed on an agent (or if `:executor-command` is not set correctly), all tasks will fail, and there will be a message in the `stderr` file for each task indicating the command the agent attempted to run.
Verify that the command is the correct command, and that it is available on each agent.
If not, either fix the `:executor-command` config, or install the executor on the agent.

If the executor is installed correctly, it will log info and errors to the `executor.log` file in the task sandbox.
This should be the first place to look for stack traces and other error information.
Each lifecycle callback is logged (`registered`, `launchTask`, etc), which can help to narrow down the issue.

When troubleshooting any issues with running the executor on the agent, it may be helpful to use a simple job command until you confirm that the executor is being launched correctly.
