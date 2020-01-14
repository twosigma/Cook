A python file server that replicates part of the Mesos `files` endpoint API for backwards compatibility.

See http://mesos.apache.org/documentation/latest/endpoints/files/download/
http://mesos.apache.org/documentation/latest/endpoints/files/read/ and
http://mesos.apache.org/documentation/latest/endpoints/files/browse/

## Building

pip install dependencies:

```bash
$ pip3 install -e .
```

## Running

Usage:

The `COOK_WORKDIR` environment variable must be set. Only files with `COOK_WORKDIR` as the root will be served.

```fileserver PORT [NUM_WORKERS]```

`NUM_WORKERS` defaults to `4`.

Examples:

```bash
$ fileserver 8000
$ fileserver 8000 4
```
