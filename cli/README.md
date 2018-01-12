## Cook CLI

### Installation

To install the Cook CLI, clone this repo and from this folder run:

```bash
python3 setup.py install
```

This will install the `cs` command on your system.

### Configuration

The Cook CLI client is designed with "end user ergonomics" in mind. 
Custom defaults may be provided for all commands. 
Multiple clusters are supported via configuration.

In order to use the Cook CLI, you’ll need a configuration file. 
`cs` looks first for a `.cs.json` file in the current directory, and then for a `.cs.json` file in your home directory. 
The path to this file may also be provided manually via the command line with the `--config` option.

There is a sample `.cs.json` file included in this directory, which looks something like this:

```json
{
  "defaults":{
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

The `defaults` map contains default values that are passed each time a command is run. 
For instance, the `submit` map above is used to set the defaults for `mem` and `cpus` when calling the `submit` command. 
These can always be overridden directly from the command line. 
For most commands, all available clusters are tried in order. 
Each entry in the `clusters` array conforms to a cluster specification ("spec"). 
A cluster spec requires a name and a url pointing to a Cook Scheduler.

### Commands

The fastest way to learn more about `cs` is with the `-h` (or `--help`) option.

All global options (`--cluster`, `--config`, etc.) can be provided when using subcommands.

#### `submit`

You can submit one or more jobs with `submit`. 
A single command can be provided as a positional argument, or multiple commands can be provided, via `stdin`. 
Jobs can also be passed as "raw" JSON data by setting the `--raw` flag. 
The `submit` command returns UUIDs, one per line, for each created job.

#### `wait`

You can wait for jobs, job instances, and job groups to complete with `wait`. 
UUIDs are passed as positional arguments.

#### `show`

You can query jobs, instances, and groups with `show`. 
UUIDs are passed as positional arguments. 
This command shows information about the queried jobs, instances, and groups. 
If you use `--json`, the command returns a JSON representation instead of tables.

#### `jobs`

The `jobs` command allows you to list all jobs:

- in a particular state (e.g. running / waiting),
- for a particular user,
- submitted within the last X hours,
- with a particular name pattern (e.g. "spark*")

As with `show`, `jobs` will list jobs across all configured clusters.

#### `ssh`

The `ssh` command accepts either a job or instance uuid and executes ssh to the corresponding Mesos agent.
Assuming the ssh connection is successful, it will also `cd` to the appropriate sandbox directory.
When given a job instance uuid, it will choose the most recently started instance's agent to ssh to.

#### `ls`

The `ls` command accepts a job or instance uuid, along with an optional path, and lists sandbox directory contents.
The path is relative to the sandbox directory on the Mesos agent where the instance runs.
When given a job instance uuid, it will choose the most recently started instance's agent to list files.

#### `tail`

The `tail` command accepts a job or instance uuid and a path, and outputs the last part of the file with that path.
The path is relative to the sandbox directory on the Mesos agent where the instance runs.
When given a job instance uuid, it will choose the most recently started instance's agent to tail files.
`tail` also supports "following" a file, meaning that it will output appended data as the file grows.

#### `kill`

You can kill jobs, instances, and groups with `kill`. 
UUIDs are passed as positional arguments.

#### `config`

You can query and set CLI configuration options with this command. 
The name is actually the sections and the key, separated by dots.

#### `cat`

The `cat` command accepts a job or instance uuid and a path, and outputs the contents of the file with that path.
As with `tail`, the path is relative to the sandbox directory on the Mesos agent where the instance runs.
When given a job instance uuid, it will choose the most recently started instance's agent to tail files.

#### `usage`

The `usage` command allows you to list cluster share and usage for a particular user.
In addition to totals, it displays a breakdown of usage by application and job group.
As with `show`, `usage` will list usage across all configured clusters.

### Examples

Simple job creation:
```shell
$ cs submit echo 1
Attempting to submit on dev0 cluster...
Job submission succeeded on dev0. Your job UUID is:
9ee74508-8b07-4b9d-88b4-9af33f8f77a0
```

Create multiple jobs, each with their own command:
```shell
$ printf 'echo 1\necho 2' | cs submit
Enter the commands, one per line (press Ctrl+D on a blank line to submit)
Attempting to submit on dev0 cluster...
Job submission succeeded on dev0. Your job UUIDs are:
6c01233c-3cf6-4994-a48a-9678b20adc35
eb9ba2ef-90dc-4f6d-a5f9-bb1b6cb4ebd9
```

Create a job from a raw JSON spec:
```shell
$ cs submit --raw
Enter the raw job(s) JSON (press Ctrl+D on a blank line to submit)
{"command": "echo 1"}
Attempting to submit on dev0 cluster...
Job submission succeeded on dev0. Your job UUID is:
cec35b58-21e4-4f67-b510-3562d660dd9e
```

Wait for a job to complete:
```shell
$ cs wait 543ed1b9-db42-445f-a731-d6761b66e4a8
```

Submit a job with a custom name, and query it:
```
$ cs submit --name ls-job ls
Attempting to submit on dev0 cluster...
Job submission succeeded on dev0. Your job UUID is:
cecf5fe6-8e92-403f-a3ff-e658fb7aeb8d

$ cs show cecf5fe6-8e92-403f-a3ff-e658fb7aeb8d

=== Job: cecf5fe6-8e92-403f-a3ff-e658fb7aeb8d (ls-job) ===

Cluster   dev0                 Attempts    1 / 1
Memory    128 MB               Job Status  Success
CPUs      1.0                  Submitted   just now
User      root                 
Priority  50                   

Command:
ls

Job Instance                          Run Time                      Host        Instance Status
3c625387-b9a6-40db-a971-f37a7eb728e6  0 seconds (started just now)  172.17.0.7  Success
```

List all jobs submitted by root, in any state, within the last (one) hour:
```bash
$ cs jobs --user root --state all --lookback 1
Cluster    UUID                                  Name                                  Memory      CPUs    Priority  Attempts    Submitted       Command    Job Status
dev0       df1ecf44-e42e-4aa3-8cc3-40a289b98666  ff9e3613-97c5-4a33-b4bd-5194fae9c29e  128 MB         1          50  1 / 1       44 minutes ago  exit 1     Failed
dev0       ec62e5c3-a2e0-454b-ada6-e31ac35ff79b  ff9e3613-97c5-4a33-b4bd-5194fae9c29e  128 MB         1          50  1 / 1       44 minutes ago  ls         Success
dev0       9bd67f93-824a-4f82-a7b1-599b3cc5a8c3  ff9e3613-97c5-4a33-b4bd-5194fae9c29e  128 MB         1          50  1 / 1       44 minutes ago  sleep 60   Success
dev0       07420e7b-915a-468f-b53b-be67debcc915  ff9e3613-97c5-4a33-b4bd-5194fae9c29e  128 MB         1          50  0 / 1       44 minutes ago  ls         Waiting
```
