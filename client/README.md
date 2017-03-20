# Cook Client
This is the code for the Cook API client. There’s a command line client as well as a Python client.

## CLI Client
The Cook CLI client was designed with “end user ergonomics” in mind. Input can be piped via `stdin` and is sent to `stdout` in order to facilitate chaining commands together. Custom defaults may be provided for all commands. Multiple clusters are supported via configuration. Basic and key based authentication are supported out of the box,  it’s also possible to implement your own.

### Installation

To install the Cook CLI, clone this repo and from this folder run:

```bash
python3 setup.py install
```

This will install the `cook` command on your system.

### Configuration

In order to use the Cook CLI, you’ll need a configuration file. `cook` looks first for a `.cook.json` file in the current directory, and then for a `cook.json` file in your home directory. This file may also be provided manually via the command line with the `—config` option.

The file looks something like this:

```json
{
  "defaults":{
    "cluster": "dev0",
    "create-job": {
      "mem": 128,
      "cpus": 1
    }
  },
  "clusters": [
    {
      "name": "dev0",
      "url": "http://127.0.0.1:12321/swagger-docs"
    },
    {
      "name": "dev1",
      "url": "http://127.0.0.1:12322/swagger-docs",
      "http_client": {
        "type": "basic_auth",
        "params": {
          "host": "127.0.0.1:12322",
          "username": "u",
          "password": "p"
        }
      }
    }
  ]
}
```

The `defaults` map contains default values that are passed each time a command is run. For instance, the `create-job` map above is used to set the default values for `mem` and `cpus` when calling the `create-job` command. These can always be overridden directly from the command line. The default `cluster` is used to determine which cluster (specified in the `clusters` array) `cook` connects to by default. If no default is provided, all available clusters are tried in order.

Each entry in the `clusters` array conforms to a cluster specification (“spec”). A cluster spec requires a name and a url pointing to the swagger spec for a Cook scheduler. Additionally, an optional `http_client` can be provided in order to add custom behavior, like authentication. If the `http_client` type takes the form of `module:function` (note the colon), the module is loaded and the function is called with the provided params. The function should return an `http_client`, and can be used (for instance) to provide custom authentication mechanisms.

### Commands

The fastest way to learn more about `cook` is with the `-h` (or `—help`) option.

```
usage: cook [-h] [--cluster CLUSTER] [--config CONFIG] [--dry-run] [--no-dry-run]
                   {create-job,create-group,kill-job,kill-instance,retry-job,await-job,await-instance,query-job,query-instance,query-group}
                   ...

positional arguments:
  {create-job,create-group,kill-job,kill-instance,retry-job,await-job,await-instance,query-job,query-instance,query-group}
    create-job          create job for command
    create-group        create group
    kill-job            kill job related to uuid
    kill-instance       kill instance related to uuid
    retry-job           retry job related to uuid
    await-job           await job related to uuid
    await-instance      await instance related to uuid
    query-job           query job related to uuid
    query-instance      query instance related to uuid
    query-group         query group related to uuid

optional arguments:
  -h, --help            show this help message and exit
  --cluster CLUSTER, -c CLUSTER
                        cluster to use
  --config CONFIG, -C CONFIG
                        config file to use
  --dry-run, -d         print action and arguments without submitting
  --no-dry-run

```

All global options (`—cluster`, `—config`, `—dry-run`) can be provided when using subcommands. `—dry-run` is useful for testing commands without actually attempting to submit.

In cases where a sub-command can take multiple positional arguments, it’s possible to pass a single dash (`-`) instead. This will cause `cook` to read input from `stdin`, with each line corresponding to one positional argument. In this way, you can pipe the output from one command into the input of another. It also makes it possible to operate on multiple entities at once. For instance, you could start multiple jobs by piping one command per line to `cook create-job`.

All options can also be provided from a file by prefixing the file path with an `@` sign. For instance, to start a job with a command read from a file: `cook create-job @command.txt`

#### `create-job`

You can create one or more jobs with `create-job`. The command is the only required option, and can be provided as a positional argument, via `stdin`, or from a file. Jobs can also be passed as “raw” JSON data by setting the `—raw` flag. This command returns UUIDs, one per line, for each created job.

```
usage: cook create-job [-h] [--uuid UUID] [--name NAME] [--priority PRIORITY]
                              [--max-retries MAX_RETRIES] [--max-runtime MAX_RUNTIME] [--cpus CPUS]
                              [--mem MEM] [--raw] [--no-raw]
                              command [command ...]

positional arguments:
  command

optional arguments:
  -h, --help            show this help message and exit
  --uuid UUID, -u UUID  uuid of job
  --name NAME, -n NAME  name of job
  --priority PRIORITY, -p PRIORITY
                        priority of job
  --max-retries MAX_RETRIES
                        maximum retries for job
  --max-runtime MAX_RUNTIME
                        maximum runtime for job
  --cpus CPUS           cpus to reserve for job
  --mem MEM             memory to reserve for job
  --raw, -r             raw job spec in json format
  --no-raw
```

#### `create-group`

You can create one or more groups with `create-group`. Groups can also be passed as “raw” JSON data as positional arguments, via `stdin`, or from a file. You can also pass multiple params (`—placement-param`, `—straggler-param`) as name value pairs, like so: `—placement-param name0=value0 —placement-param name1=value1`. This command returns UUIDs, one per line, for each created group.

```
usage: cook create-job [-h] [--uuid UUID] [--name NAME] [--priority PRIORITY]
                              [--max-retries MAX_RETRIES] [--max-runtime MAX_RUNTIME] [--cpus CPUS]
                              [--mem MEM] [--raw] [--no-raw]
                              command [command ...]

positional arguments:
  command

optional arguments:
  -h, --help            show this help message and exit
  --uuid UUID, -u UUID  uuid of job
  --name NAME, -n NAME  name of job
  --priority PRIORITY, -p PRIORITY
                        priority of job
  --max-retries MAX_RETRIES
                        maximum retries for job
  --max-runtime MAX_RUNTIME
                        maximum runtime for job
  --cpus CPUS           cpus to reserve for job
  --mem MEM             memory to reserve for job
  --raw, -r             raw job spec in json format
  --no-raw
 λ  python3 -m cook create-group -h
usage: __main__.py create-group [-h] [--uuid UUID] [--name NAME] [--placement-type PLACEMENT_TYPE]
                                [--placement-param PLACEMENT_PARAMS] [--straggler-type STRAGGLER_TYPE]
                                [--straggler-param STRAGGLER_PARAMS]
                                [spec [spec ...]]

positional arguments:
  spec

optional arguments:
  -h, --help            show this help message and exit
  --uuid UUID, -u UUID  uuid of job group
  --name NAME, -n NAME  name of job group
  --placement-type PLACEMENT_TYPE, -pt PLACEMENT_TYPE
                        host placement type for job group
  --placement-param PLACEMENT_PARAMS, -pp PLACEMENT_PARAMS
                        host placement type for job group
  --straggler-type STRAGGLER_TYPE, -st STRAGGLER_TYPE
                        straggler handling type for job group
  --straggler-param STRAGGLER_PARAMS, -sp STRAGGLER_PARAMS
                        straggler handling type for job group
```

#### `kill-job`

You can kill jobs with `kill-job`. Job UUIDs can be passed as positional arguments, via `stdin`, or from a file. This command exits with a status of 0 if the jobs have been killed.

```
usage: cook kill-job [-h] uuid [uuid ...]

positional arguments:
  uuid

optional arguments:
  -h, --help  show this help message and exit
```

#### `kill-instance`

You can kill instances with `kill-instance`. Instance UUIDs can be passed as positional arguments, via `stdin`, or from a file. This command exits with a status of 0 if the instances have been killed.

```
usage: cook kill-instance [-h] uuid [uuid ...]

positional arguments:
  uuid

optional arguments:
  -h, --help  show this help message and exit
```

#### `retry-job`

You can retry jobs with `retry-job`. Job UUIDs can be passed as positional arguments, via `stdin`, or from a file. This command returns the retry count, one per line, for each retried job.

```
usage: cook retry-job [-h] [--max-retries MAX_RETRIES] uuid [uuid ...]

positional arguments:
  uuid

optional arguments:
  -h, --help            show this help message and exit
  --max-retries MAX_RETRIES
                        maximum retries for job

```

#### `await-job`

You can wait for jobs with `await-job`. Job UUIDs can be passed as positional arguments, via `stdin`, or from a file. This command returns a JSON representation of a job, one per line, for each awaited job.

```
usage: cook await-job [-h] [--timeout TIMEOUT] [--interval INTERVAL] uuid [uuid ...]

positional arguments:
  uuid

optional arguments:
  -h, --help            show this help message and exit
  --timeout TIMEOUT, -t TIMEOUT
                        maximum time to wait for a job to complete
  --interval INTERVAL, -i INTERVAL
                        time to wait between polling for a job
```

#### `await-instance`

You can wait for instances with `await-instance`. Instance UUIDs can be passed as positional arguments, via `stdin`, or from a file. This command returns a JSON representation of an instance, one per line, for each awaited instance.

```
usage: cook await-instance [-h] [--timeout TIMEOUT] [--interval INTERVAL] uuid [uuid ...]

positional arguments:
  uuid

optional arguments:
  -h, --help            show this help message and exit
  --timeout TIMEOUT, -t TIMEOUT
                        maximum time to wait for an instance to complete
  --interval INTERVAL, -i INTERVAL
                        time to wait between polling for an instance
```

#### `query-job`

You can query jobs with `query-job`. Job UUIDs can be passed as positional arguments, via `stdin`, or from a file. This command returns a JSON representation of a job, one per line, for each queried job.

```
usage: cook query-job [-h] uuid [uuid ...]

positional arguments:
  uuid

optional arguments:
  -h, --help  show this help message and exit
```

#### `query-instance`

You can query instances with `query-instance`. Instance UUIDs can be passed as positional arguments, via `stdin`, or from a file. This command returns a JSON representation of an instance, one per line, for each queried instance.

```
usage: cook query-instance [-h] uuid [uuid ...]

positional arguments:
  uuid

optional arguments:
  -h, --help  show this help message and exit
```

#### `query-group`

You can query groups with `query-group`. Group UUIDs can be passed as positional arguments, via `stdin`, or from a file. This command returns a JSON representation of a group, one per line, for each queried group.

```
usage: cook query-group [-h] [--detailed] [--no-detailed] uuid [uuid ...]

positional arguments:
  uuid

optional arguments:
  -h, --help      show this help message and exit
  --detailed, -r  show additional group statistics
  --no-detailed
```

### Examples

Simple job creation:
```shell
cook create-job echo 1
```

Create multiple jobs, each with their own command:
```shell
printf 'echo 1\necho 2' | cook create-job -
```

Create a job from a raw JSON spec:
```shell
echo '{"command": "echo 1"}' | cook create-job -
```

Simple job creation, with command from a file:
```shell
cook create-job @command.txt
```

Simple job creation, and wait for job to complete:
```shell
cook create-job echo 1 | cook await-job -
```

## Python Client
The Cook python client is really two different interfaces. One is a low-level client (`CookClient`, and the `CookMultiClient` wrapper), and the other is a set of convenience classes for working with Cook’s primary entities: `Group`, `Job`, and `Instance`.

### Classes

#### `CookClient`

The `CookClient` class is a low-level interface to the cook API. It’s constructor accepts a swagger spec url:

```python
client = CookClient(url='http://127.0.0.1:12321/swagger-docs')
```

Each method returns `(okay, result)`, where `okay` is a boolean indicating whether the request has succeeded or failed.

##### `create_jobs(jobs)`

Create a list of job specs and return their UUIDs.

##### `create_groups(groups)`

Create a list of groups and return their UUIDs.

##### `delete_jobs(uuids)`

Deletes a list of jobs by UUID.

##### `delete_instances(uuids)`

Deletes a list of instances by UUID.

##### `poll_jobs(uuids)`

Polls a list of jobs by UUID, and returns them.

##### `poll_instances(uuids)`

Polls a list of instances by UUID, and returns them.

##### `poll_groups(uuids, detailed=False)`

Polls a list of groups by UUID, and returns them. If `detailed` is set to `True`, additional group stats are also returned.

##### `await_jobs(uuids, timeout=30, interval=5)`

Waits for a list of jobs by UUID, and returns them.

##### `await_instances(uuids, timeout=30, interval=5)`

Waits for a list of instances by UUID, and returns them.

##### `retry_jobs(uuids)`

Retries a list of jobs by UUID, and returns their retry count.

#### `CookMultiClient`

The `CookMultiClient` provides all the same methods as the `CookClient` but its constructor accepts multiple “client specs”, all of which are tried in turn when making a request, until one succeeds, or all have failed:

```python
client = CookMultiClient([
	{'url': 'http://127.0.0.1:12321/swagger-docs},
	{'url': 'http://127.0.0.1:12322/swagger-docs'}
])
```

#### `Group`

The `Group` class can be used to create and view a group. To create a `Group` instance for an existing group, pass a `uuid` in addition to an instance of a `CookClient`:

```python
group = Group(client, uuid=SOME_UUID)

# data is automatically synced
print(group.name)
```

To create a group, don’t pass a `uuid`:

```python
group = Group(client)

# group is automatically created
print(group.uuid)
```


#### `Job`

The `Job` class can be used to create and interact with a job. To create a `Job` instance for an existing job, pass a `uuid` in addition to an instance of a `CookClient`:

```python
job = Job(client, uuid=SOME_UUID)

# data is automatically synced
print(job.name)
```

To create a group, don’t pass a `uuid`, but do pass a `command`:

```python
job = Job(client, command='echo 1')

# job is automatically created
print(job.uuid)
```

Additionally, a job can be killed by calling  `kill`, retried by calling `retry`, and waited for by calling `wait`.

#### `Instance`

The `Instance` class can be used to view an instance. For instance, to create a `Instance` instance for an existing instance, pass a `uuid` in addition to an instance of a `CookClient` this instant:

```python
instance = Group(client, uuid=SOME_UUID)

# data is automatically synced
print(instance.status)
```

Additionally, an instance can be killed by calling `kill`, and waited for by calling `wait`.