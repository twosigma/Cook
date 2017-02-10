Cook Executor
=============

The cook executor is a custom executor written in python. It replaces the default command executor in order to enable a number of features for both operators and end users.

For Users
-----

The cook executor supports a number of (completely optional) features beyond the standard Mesos command executor. It allows users to specify additional commands that run either before or after a job's primary command. This enables the use of existing utilities, and promotes sharing and reuse. It also provides an HTTP API that allows commands to expose progress updates and custom failure messages, as well as update environment variables.

### Scheduler HTTP API

#### `POST /rawscheduler`

The job creation endpoint can be used to specify before and after commands, with the following keys:

| key | type | description |
|-----|------|-------------|
| `before_commands` | array | An array of `command`s to run before all user specified commands. |
| `after_commands` | array | An array of `command`s to run after all user specified commands. |

Commands have the following structure:

| key | type | description |
|-----|------|-------------|
| `value` | string | A string representing the command to be run, ex: `echo hello`. |
| `guard` | boolean | An optional flag. If set, and the command returns a non-zero exit code, no further commands for the job will be executed. |
| `async` | boolean | An optional flag. If set, the command runs asynchronously, in parallel with other future commands. It will be killed when all synchronous commands for a job are finished. |

#### `GET /rawscheduler?job=:job`

The job status endpoint can be used to view a job's progress, custom failure message, as well as the exit codes for all commands, by examing the following additional keys:

| key | type    | description                              |
| -------- | ------- | ---------------------------------------- |
| `exit_code`     | integer | Status code when job specifies `command`  (will be absent if job has yet to finish). |
| `before_exit_codes` | array of integers | Status codes for `before_commands`. |
| `after_exit_codes` | array of integers | Status codes for `after_commands`. |
| `progress` | long | Progress reported by a job's commands via the executor HTTP API. May be absent. |
| `message` | string | A custom failure message set by a command using the executor HTTP API. May be absent. |

### Executor HTTP API

The executor provides an HTTP API that can be used from a job's commands. It currently only supports two endpoint:

#### `PATCH /task/:taskid`

This request also requires a body. Since it is a `PATCH` request, a valid request may contain only some of the following keys:

| key | type   | description                              |
| -------- | ------ | ---------------------------------------- |
| `progress` | float  | A number between 0 and 100, representing the progress of the task |
| `message`   | string | A custom failure reason, expected to be accompanied by a non-zero exit code. |
| `env`      | object | A map of env variable names to values, a null value will unset the variable. This is really only useful from a before command, since it will only affect commands that run afterward.  |

Example - progress update:

`curl -X PATCH -d '{progress: 10}' $EXECUTOR_ENDPOINT`

Example - setting and unsetting environment variables:

`curl -X PATCH -d '{env: {VAR_A: "123", SOME_B: null}}' $EXECUTOR_ENDPOINT`

Potential responses:

| Code | HTTP Meaning        | Possible reason                          |
| ---- | ------------------- | ---------------------------------------- |
| 204  | No Content          | The request was succesfully processed. |
| 400  | Malformed           | The request body contains an unknown key, or a key's value is the wrong type. |
| 503  | Service Unavailable | The request was valid, and received, but there was an (ostensibly temporary) failure with mesos itself. |

#### `GET /task/:taskid`

For successful requests, the response body could contain:

| key | type | description |
|-----|------|-------------|
| `exit_code` | int | The exit code of the primary command. |
| `before_codes` | array of ints | The exit codes of the before commands. |
| `after_codes` | array of ints | The exit codes of the after commands. |

Potential responses:

| Code | HTTP Meaning        | Possible reason                          |
| ---- | ------------------- | ---------------------------------------- |
| 200  | Okay          | The task id was valid, and the response body contains task data |
| 404  | Not found           | The provided task id was unknown or invalid |
| 503  | Service Unavailable | The request was valid, and received, but there was an (ostensibly temporary) failure with mesos itself. |

### Example use cases

Below are some examples of how to use the above features to solve some common problems.

#### Conditionally preventing command execution

In cases where only one copy of a certain job should be running, before and after commands could be used to acquire and release a lock. Assuming you've written some scripts for managing your lock, you could execute them by passing the additional data to the cook scheduler's job creation API:

```json
{
	"command": "my-actual-command",
	"before_commands": [{
		"value": "./acquire-zk-lock.sh my-service"
		"guard": true
	}],
	"after_commands":[{
		"value": "./release-zk-lock.sh my-service"
	}],
	"uris": [{
		"value": "http://my-file-server/release-zk-lock.sh",
		"executable": true
  	},{
  		"value": "http://my-file-server/acquire-zk-lock.sh",
  		"executable": true
  	}]
}
```

The `guard` flag is used to prevent execution of later commands if we can't acquire the lock (in this case, `acquire-zk-lock.sh` script will return a non-zero exit code). Note that the scripts for managing the lock could be completely reusable, and that the primary command did not need to change.

#### Monitoring a running command

Perhaps you'd like to tail a log created by your primary command. You could use an asynchronous before command to do so. It will be started before your primary command, and continue running in the background until the primary command (and all other synchronous commands) finish.

```json
{
	"command": "my-actual-command",
	"before_commands": [{
		"value": "./my-monitoring-command",
		"async": true
	}]
}
```

#### Dynamically setting environment variables

In order to dynamically set environment variables for later commands, you can use the executor HTTP API from a before command.

```json
{
	"command": "my-actual-command",
	"before_commands": [{
		"value": "./set-env-vars.sh"
	}]
}
```

The before command would need to executor something like the following:

```bash
curl -XPATCH \
	-d '{"env": {"MY_ENV_VAR": "/hello/world", "MY_OLD_VAR": null}}' \
	$EXECUTOR_ENDPOINT
```

This would set `MY_ENV_VAR` for your primary command (and all commands after `set-env-vars.sh`.

#### Update progress and custom failure messages

In order to update progress or add a custom failure message, you can use the executor HTTP API from any command, with something like the following:

```bash
curl -XPATCH \
	-d '{"progress": 100, "message": "a custom failure message"}' \
	$EXECUTOR_ENDPOINT
```

#### Sending an email on completion

In order to send an email once your primary command has finished, you can use an after command:

```json
{
	"after_commands": [{
		"value": "mail -s 'your job failed, sorry' bob@hello.com"
	}]
}
```

You could also take advantage of the read API to check the exit code of the primary command in order to only send an email on error. Here's an example script using `bash` and `jq`:

```bash
exit_code=$(curl $EXECUTOR_ENDPOINT | jq '.exit_code')

if [[ "$exit_code" != "0" ]]; then
    mail -s 'your job failed, sorry!' bob@hello.com
fi
```

For Operators
---------

The cook executor replaces the default command executor. It can either be installed on each agent, or downloaded on each agent via a `uri`. As the executor communicates with Mesos agent via HTTP, the agent must support the HTTP API and have it enabled. Primarily, the operator will interact with the executor by setting default commands that run either before or after the commands specified by users when submitting a job to cook. For guidance on configuring these commands, see the configuration section below.

### Installation

First, make sure `python3` is installed on each agent.

Then, to install the executor executable (`cook-executor`) and all dependencies, run:

```bash
./setup.py install
```

Alternatively, [`pyinstaller`](https://github.com/pyinstaller/pyinstaller) can be used to build a single executable that is distributed via a URI. For example, to build the executable, run the following command from the `executor` folder:

```bash
# will create dist/cook-executor
pyinstaller -F -n cook-executor -p cook cook/__main__.py
```

### Configuration

Configuration for the executor should go under the `:executor` key of the edn configuration file for the cook scheduler.

For example:

```clojure
{...
 :executor {:command "cook-executor"
            :max-message-length 512
            :log-level "INFO"
            :before-commands [{:value "echo before"}]
            :after-commands [{:value "echo after"}]
            :uris [{:value "/path/to/executor/executable"}]}
 ...}
```

Supported configuration options:

| option | type | description |
|--------|------|-------------|
| `:command` | string | A string containing the command executed on the mesos agent to launch the cook executor. If the executor is installed using the instructions above, the default value of `cook-executor` should not need to be changed. |
| `:log-level` | string | The log level for the executor process. Defaults to "INFO" |
| `:max-message-length` | long | The maximum length for the custom failure message set by a task via the executor HTTP API. The default is 512. |
| `:before-commands` | vector | An optional vector of `command`s to run before all user specified commands. |
| `:after-commands` | vector | An optional vector of `command`s to run after all user specified commands. |
| `:uris` | vector | An optional vector of `uri`s to be associated with every job |

`command`s have the following structure:

| key | type | description |
|-----|------|-------------|
| `:value` | string | A string representing the command to be run, ex: `echo hello`. |
| `:guard` | boolean | An optional flag. If set, and the command returns a non-zero exit code, no further commands for the job will be executed. |
| `:async` | boolean | An optional flag. If set, the command runs asynchronously, in parallel with other future commands. It will be killed when all synchronous commands for a job are finished. |

`uri`s have the following structure:

| key | type | description |
|-----|------|-------------|
| `:value` | string | The URI to fetch. Supports everything the Mesos fetcher supports, i.e. http://, https://, ftp://, file://, hdfs:// |
| `:executable` | boolean | Should the URI have the executable bit set after download? |
| `:extract` | boolean | Should the URI be extracted (must be a tar.gz, zipfile, or similar).
| `:cache` | boolean | Mesos 0.23 and later only: should the URI be cached in the fetcher cache? |

### Tests

The cook executor uses `pytest`. To install test dependencies and run the executor test suite, run:

```bash
./setup.py test
```

### Troubleshooting

If the executor is not correctly installed on an agent (or if `:executor-command` is not set correctly), all tasks will fail, and there will be a message in the `stderr` file for each task indicating the command the agent attempted to run. Verify that the command is the correct command, and that it is available on each agent. If not, either fix the `:executor-command` config, or install the executor on the agent.

If the executor is installed correctly, it will log info and errors to the `executor.log` file in the task sandbox. This should be the first place to look for stack traces and other error information. Each lifecycle callback is logged (`registered`, `launchTask`, etc), which can help to narrow down the issue.

When troubleshooting any issues with running the executor on the agent, it may be helpful to remove the `:job-defaults` entirely until you confirm that executor is being launched correctly. It can also be helpful to simplify commands for example `echo before` to rule out problems with the commands themselves.
