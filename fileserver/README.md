A python file server that replicates part of the Mesos `files` endpoint API for backwards compatibility.

See http://mesos.apache.org/documentation/latest/endpoints/files/read/ and http://mesos.apache.org/documentation/latest/endpoints/files/download/

## Building

pip install dependencies:

```bash
$ pip3 install -r requirements.txt
```

## Running

Run ```bin/run-file-server```; see file for usage details.

```bash
$ ./bin/run-file-server -p 8000
```
