# The Cook Scheduler Python Client API

This package defines a client API for Cook Scheduler, allowing Python applications to easily integrate with Cook.

## Quickstart

The code below shows how to use the client API to connect to a Cook cluster listening on `localhost:12321`, submit a job to the cluster, and query its information.

```python
from cookclient import JobClient

client = JobClient('localhost:12321')

uuid = client.submit(command='ls')
job = client.query(uuid)
print(str(job))
```
