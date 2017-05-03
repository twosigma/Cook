## Cook CLI

### Installation

To install the Cook CLI, clone this repo and from this folder run:

```bash
python3 setup.py install
```

This will install the `cs` command on your system.

### Configuration

The Cook CLI client is designed with "end user ergonomics" in mind. Custom defaults may be provided for all commands. Multiple clusters are supported via configuration.

In order to use the Cook CLI, you’ll need a configuration file. `cs` looks first for a `.cook.json` file in the current directory, and then for a `cook.json` file in your home directory. This file may also be provided manually via the command line with the `--config` option.

The file looks something like this:

```json
{
  "defaults":{
    "cluster": "dev0",
    "submit": {
      "mem": 128,
      "cpus": 1
    }
  },
  "clusters": [
    {
      "name": "dev0",
      "url": "http://127.0.0.1:12321/"
    },
    {
      "name": "dev1",
      "url": "http://127.0.0.1:12322/"
    }
  ]
}
```

The `defaults` map contains default values that are passed each time a command is run. For instance, the `submit` map above is used to set the default values for `mem` and `cpus` when calling the `submit` command. These can always be overridden directly from the command line. The default `cluster` is used to determine which cluster (specified in the `clusters` array) `cs` connects to by default. If no default is provided, all available clusters are tried in order. Each entry in the `clusters` array conforms to a cluster specification ("spec"). A cluster spec requires a name and a url pointing to a Cook scheduler.

### Commands

The fastest way to learn more about `cs` is with the `-h` (or `—help`) option.

```
usage: cs [-h] [--cluster CLUSTER] [--url URL] [--config CONFIG]
          [--retries RETRIES] [--verbose]
          {submit,wait,show} ...

cs is the Cook Scheduler CLI

positional arguments:
  {submit,wait,show}
    submit              create job for command
    wait                wait for job(s) to complete by uuid
    show                show job(s) by uuid

optional arguments:
  -h, --help            show this help message and exit
  --cluster CLUSTER, -c CLUSTER
                        the name of the Cook scheduler cluster to use
  --url URL, -u URL     the url of the Cook scheduler cluster to use
  --config CONFIG, -C CONFIG
                        the configuration file to use
  --retries RETRIES, -r RETRIES
                        the number of retries to use for HTTP connections
  --verbose, -v         be more verbose/talkative (useful for debugging)
```

All global options (`--cluster`, `--config`, etc) can be provided when using subcommands.

#### `submit`

You can submit one or more jobs with `submit`. A single command can be provided as a positional argument, or multiple commands can be provided, one per line, via `stdin`. Jobs can also be passed as "raw" JSON data by setting the `--raw` flag. The `submit` command returns UUIDs, one per line, for each created job.

```
$ cs submit --help
usage: cs submit [-h] [--uuid UUID] [--name NAME] [--priority]
                 [--max-retries MAX-RETRIES] [--max-runtime MAX-RUNTIME]
                 [--cpus CPUS] [--mem MEM] [--raw]
                 [command] ...

positional arguments:
  command
  args

optional arguments:
  -h, --help            show this help message and exit
  --uuid UUID, -u UUID  uuid of job
  --name NAME, -n NAME  name of job
  --priority , -p       priority of job, between 0 and 100 (inclusive)
  --max-retries MAX-RETRIES
                        maximum retries for job
  --max-runtime MAX-RUNTIME
                        maximum runtime for job
  --cpus CPUS           cpus to reserve for job
  --mem MEM             memory to reserve for job
  --raw, -r             raw job spec in json format
```

#### `wait`

You can wait for jobs to complete with `wait`. Job UUIDs are passed as positional arguments.

```
$ cs wait --help
usage: cs wait [-h] [--timeout TIMEOUT] [--interval INTERVAL] uuid [uuid ...]

positional arguments:
  uuid

optional arguments:
  -h, --help            show this help message and exit
  --timeout TIMEOUT, -t TIMEOUT
                        maximum time (in seconds) to wait
  --interval INTERVAL, -i INTERVAL
                        time (in seconds) to wait between polling
```

#### `show`

You can query jobs with `show`. Job UUIDs are passed as positional arguments. This command returns a table showing the fields and values for the queried jobs. If you use `--json`, the command returns a JSON representation of the queried jobs.

```
$ cs show --help
usage: cs show [-h] [--json] uuid [uuid ...]

positional arguments:
  uuid

optional arguments:
  -h, --help  show this help message and exit
  --json      show the job(s) in JSON format
```

### Examples

Simple job creation:
```shell
$ cs submit echo 1
ddeebd40-656c-44c4-9843-9e5e8e06ae5a
```

Create multiple jobs, each with their own command:
```shell
$ printf 'echo 1\necho 2' | cs submit
Enter the commands, one per line (press Ctrl+D on a blank line to submit)
7473ecdb-3931-45e0-952e-9666d6c67d77
d0b9b056-ea76-4708-b5e4-d53b5f5300a1
```

Create a job from a raw JSON spec:
```shell
$ cs submit --raw '{"command": "echo 1"}'
e47a496c-7226-4083-8988-44c262e0664b
```

Simple job creation, and wait for job to complete:
```shell
$ cs wait $(cs submit echo 1)
```

Submit a job with a custom name, and immediately query it:
```
$ cs show --json $(cs submit --name ls-job ls) | jq
[
  {
    "env": {},
    "status": "waiting",
    "priority": 50,
    "labels": {},
    "submit_time": 1495814775770,
    "mem": 128,
    "ports": 0,
    "uris": [],
    "cpus": 1,
    "max_runtime": 9223372036854776000,
    "state": "waiting",
    "command": "ls",
    "gpus": 0,
    "name": "ls-job",
    "retries_remaining": 1,
    "user": "root",
    "framework_id": null,
    "instances": [],
    "max_retries": 1,
    "uuid": "055ea09c-768d-408d-92e2-93110e1bfb34"
  }
]
```
