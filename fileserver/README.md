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

Run ```fileserver```; see bin/fileserver for usage details.

```bash
$ fileserver 8000
```
