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

For development, a docker-compatible version of the cook executor can be built using:

```bash
# will create dist/cook-executor
$ bin/build-cook-executor.sh
```

### Configuration

Configuration for the executor should go under the `:executor` key of the edn configuration file for the cook scheduler.

For example:

```clojure
{...
 :executor {:command "cook-executor"
            :log-level "INFO"
            :max-message-length 512
            :progress-output-name "stdout"
            :progress-regex-string "\^\^\^\^JOB-PROGRESS: (\d*)(?: )?(.*)"
            :progress-sample-interval-ms 1000
            :uri {:cache true
                  :executable true
                  :extract false
                  :value "file:///path/to/cook-executor"}}
 ...}
```

Supported configuration options:

| option | type | description |
|--------|------|-------------|
| `:command` | string | A string containing the command executed on the mesos agent to launch the cook executor. If the executor is installed using the instructions above, the default value of `cook-executor` should not need to be changed.|
| `:log-level` | string | The log level for the executor process. Defaults to "INFO".|
| `:max-message-length` | long | The maximum length for the custom failure message set by a task via the Mesos executor HTTP API. The default is 512.|
| `:progress-output-name` | string | The file to track for progress updates. The default is stdout.|
| `:progress-regex-string` | string | The regex used to identify progress update messages. The regex should have two capture groups, the first being an integer representing the progress percent. The second being a message about the progress. Defaults to "progress: (\d*), (.*)".|
| `:progress-sample-interval-ms` | long | The interval in ms after which to send progress updates. The default is 1000.|
| `:uri` | map | A description of the `uri` used to download the executor executable. The `uri` structure is defined below.|

`uri`s have the following structure:
| key | type | description |
|-----|------|-------------|
| `:cache` | boolean | Mesos 0.23 and later only: should the URI be cached in the fetcher cache? |
| `:executable` | boolean | Should the URI have the executable bit set after download? |
| `:extract` | boolean | Should the URI be extracted (must be a tar.gz, zipfile, or similar).
| `:value` | string | The URI to fetch. Supports everything the Mesos fetcher supports, i.e. http://, https://, ftp://, file://, hdfs:// |

### Tests

The cook executor uses `nose`.
To install test dependencies and run the executor test suite, run:

```bash
$ python setup.py nosetests
```

If you want to run a single test and see the log messages as they occur, you can run, for example:

```bash
$ nosetests tests.test_executor:ExecutorTest.test_get_task_id --nologcapture
```

### Troubleshooting

If the executor is not correctly installed on an agent (or if `:executor-command` is not set correctly), all tasks will fail, and there will be a message in the `stderr` file for each task indicating the command the agent attempted to run.
Verify that the command is the correct command, and that it is available on each agent.
If not, either fix the `:executor-command` config, or install the executor on the agent.

If the executor is installed correctly, it will log info and errors to the `executor.log` file in the task sandbox.
This should be the first place to look for stack traces and other error information.
Each lifecycle callback is logged (`registered`, `launchTask`, etc), which can help to narrow down the issue.

When troubleshooting any issues with running the executor on the agent, it may be helpful to use a simple job command until you confirm that the executor is being launched correctly.
