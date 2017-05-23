# Cook Executor

The document outlines a specification for Cook's custom executor.

It is a modular design allowing users to add functionality by writing reusable, single-responsibility processes. Declarative configuration would determine when then processes would be executed. An HTTP API exposed by the executor itself would enable communication between the executor and the tasks it starts.


## Scheduler HTTP API

The following enhancements to the existing Cook scheduler API will:

1. Allow users to specify multiple commands when creating jobs
2. Allow users to query the status code or codes of a completed job

### Launch a Job

It should be possible to specify multiple commands when launching a job. Any command failure will fail the whole job. In the future, it may be desirable to have finer-grained control over how failure will be handled (for example, ignoring failures from asynchronous monitoring commands).

Request body schema additions:

| JSON Key | Type | Description |
|----------|------|-------------|
| commands | array of command objects | the commands to run, as well as information about how they are to be run |

`command` object schema:

| JSON Key | Type | Description |
|----------|------|-------------|
| command  | string | the command to run |
| async | boolean | a flag determining whether the command is run asynchronously.  defaults to `false` |
| guard | boolean | a flag determining whether failure of the command will stop execution of the whole job. defaults to `false` |

Example `commands`:


    commands: [
      {
        async: true,
        command: “tail_logs.py”
      },
      {
        guard: true,
        command: “ensure_only_one_job.sh”
      },
      {
        command: "actual_command.py"
      },
      {
        command: "send_email.pl"
      }
    ]

Most return codes won’t change, but since `command` and `commands` are mutually exclusive, there’s a new way to get a `400` response.

| Code | HTTP Meaning | Possible reason |
|------|--------------|-----------------|
| 400  | Malformed    | if both `command` and `commands` are set for a job |


### Query Status of a Job

Querying a job’s status also should return its exit code (or codes).

Response schema additions:

| JSON Key | Type | Description |
|----------|------|-------------|
| code | integer | status code when job specifies `command`  (will be absent if job specifies `commands`, or if job has yet to finish) |
| codes | array of integers | status codes when job specifies `commands` (will be absent if job specifies `command`, or if job has yet to finish) |

No changes to return codes are necessary.

## Scheduler Datomic API

It will also be necessary to change the Datomic schema in order to in order to support users of Cook’s “Datomic API”.

Schema additions to support multiple commands per job:

| Attribute | Type | Cardinality | Description |
|-----------|------|-------------|-------------|
| :job/command | ref | many | reference to a `command` entity |
| :command/line | string | one | command line to run |
| :command/code | double | one | exit code of command |
| :command/params | ref | many | reference to a `command.param` entity |
| :command.param/key | string | one | a command param key, like `async` |
| :command.param/value | string | one | a command param value, like `true` |


## Mesos executor callbacks

Cook's executor will need to implement the following mesos callbacks:

`registered` :

No action taken, just logged. In the future, may be used to pass configuration from the scheduler to the executor via ExecutorInfo’s data field.

`reregistered` :

No action taken, just logged.

`disconnected` :

No action taken, just logged.

`launchTask` :

The task status is updated to “running”.  An ordered list of commands is deserialized the from data field of TaskInfo. These commands are run in order, in their own process. Synchronous commands (the default) must exit before running the next command is run. Asynchronous commands (specified by setting the `async` flag for a command) are started in order, but run in parallel with other commands. For all commands, the exit code is captured and stored in the data field of the TaskStatus. By default, commands with non-zero (error) exit codes have no effect on other commands. For “guard” commands (specified by setting the `guard` flag for a command), a non-zero exit code causes the task to be marked as “failed” and prevents other commands from running. When the last synchronous command has finished, the task is marked as “finished. Any asynchronous commands that are still running are sent a SIGTERM, followed in 10s by a SIGKILL if the command did not exit. Like Mesos' existing command executor, STDOUT and STDERR would be redirected to files in the task sandbox.

`killTask` :

The processes associated with TaskID are sent a SIGTERM, followed in 10s by a SIGKILL. The task status is updated to “killed”.

`frameworkMessage` :

No action taken, just logged.

`shutdown` :

All tasks are killed. The processes for each task are sent a SIGTERM, followed in 10s by a SIGKILL. Each task status is updated to “killed”.

`error` :

No action taken, just logged.


## Executor HTTP API

There must be some API between the executor and the processes it start. At the very least, it must enable the ability for a task to report its progress to the executor. There are a number of potential API designs that could satisfy these constraints. An HTTP API on the executor itself seems like a good balance of complexity, since even though it makes the executor more complicated (by requiring an HTTP server), it provides a flexible interface that is simple to consume by the task. Some alternate approaches (along with their pros and cons) are outlined below.

### Updating a tasks's state


`PATCH /task/:taskid`

This request also requires a body. Since it is a `PATCH` request, a valid request may contain only some of the following keys. This data is stored directly on the TaskInfo.


Request body keys:

| JSON Key | Type | Description |
|----------|------|-------------|
| progress | float | a number between 0 and 100, representing the progress of hte task |
| reason | string | a custom failure reason, expected to be accompanied by a non-zero exit code |

Example usage:

`curl -X PATCH -d '{progress: 10}' $EXECUTOR_BASE_URL/task/1`

Potential responses:

| Code | HTTP Meaning | Possible reason |
|------|--------------|-----------------|
| 204 | No Content | the request was succesfully processed |
| 400 | Malformed | the request body contains an unknown key, or a key's value is the wrong type |
| 503 | Service Unavailable | the request was valid, and received, but there was an (ostensibly temporary) failure with mesos itself |


## "Process" API Pro/Cons

As mentioned above, here are some potential alternative APIs for communication between the executor and the tasks.

### Executor File API

A file or fifo is used to send messages from the task to the executor. The file path is sent to the task via an environment variable. Messages are appended to the file, which are then read by the executor.

Pros:

- Simple authentication (a file or pipe writable by the user)
- Simple discovery (either a well known name, or name passed via the environment)
- Easy to inspect/debug contents of “messages” file
- Resiliant to scheduler failure
- Does not require changes to existing commands
- Does not couple tasks to Cook for execution

Cons:


- Data can only flow in one direction
- Forces all communication to be asynchronous
- Easy to mangle files (mitigated by using fifo, but lose ease of inspection)
- Complicates the executor (now needs to keep track of and monitor files for changes, and send changes back to scheduler)


### Executor HTTP API

The executor provides an HTTP API over a port or unix socket, details of which are shared via an environment variable. The task uses an HTTP client to hit the API. Authentication is not necessary, as access to the port or socket is not available outside the container in which the executor is running.

Pros:

- Simple authentication (since executor is in same container as other task processes, could likely bind to a loopback address, or use a unix socket)
- Simple discovery (could pass address or socket via the environment, or use a well known name for socket)
- Resiliant to scheduler failure 

Cons:

- Complicates the executor (now needs to run an http server, and send messages back to scheduler)
- Couples tasks to Cook for execution


### Scheduler HTTP API

The executor could avoid exposing any API, and instead the existing scheduler HTTP API could be enhanced to handle the task state updates. The location of the scheduler could be passed to the task via an envirnoment variable, but there would be problems if the scheduler moved. The task would still need to authenticate, and while it's likely it has access to the necessary credentials, SPNEGO support for HTTP clients is less ubiquitous.

Pros:

- One unified API for managing cook
- Simplest possible executor

Cons:

- Potentially more complicated discovery (though likely solved since the schedular API is already available to users)
- Potentially more complicated authentication story (though likely solved by mesos’ kerberos support; is there a TGT in the credential cache?)
- Makes the task’s ability to record data dependent on the Cook scheduler


## Implementation Language Pro/Cons

Mesos launches one executor process per container. Cook runs each task in its own container. This means that any resources consumed by the executor will be added to the task. It also means that executor startup time will add to the perceived latency between scheduling a job in Cook, and mesos actually running a task. While this may not matter much for long running, resource intensive tasks, it could make a big difference for short lived, light-weight tasks. Python seems like a good balance of performance and ease of development. It's popularity inside Two Sigma would make internal contribution more likely. It's popularity outside of Two Sigma would make it less likely to deter potential open source contributors.

### Clojure

Most of Cook is already written in Clojure, everyone's favorite lisp for the JVM.

Pros:

- Keeps Cook in (mostly) one language, which may make open source contributions more likely
- Has a libmesos API wrapper (via Mesomatic)

Cons:

- Relatively slow startup time
- Relatively high memory footprint

### Python

Python is a popular interpreted language inside (and outside) of Two Sigma.

Pros:

- Most popular language inside Two Sigma
- No HTTP API wrapper, but has a first-party libmesos API wrapper
- Likely capable of sufficiently low startup latency
- Likely capable of sufficiently low memory footprint

Cons:

- Adds another language to the Cook repo, which may deter contributors

### Go

Go sits between C++ and Python (it's compiled like C++, but garbage collected like Python).

Pros:

- Has an existing (albeit third party) HTTP API wrapper for the executor, which is the recommended approach for building an executor as of Mesos 1.0
- Likely capable of sufficiently low startup latency
- Likely capable of sufficiently low memory footprint

Cons:

- Adds another language to the Cook repo, which may deter contributors
- Not much adoption inside Two Sigma (not sure if this is true)

### C++

Mesos is written in C++, and likely the lowest level language worth considering. 

Pros:

- Currently has the only existing first-party HTTP API wrapper
- Likely capable of the lowest startup latency

Cons:

- A relatively low level language, which will likely cause longer development time
- Adds another language to the Cook repo, which may deter contributors
