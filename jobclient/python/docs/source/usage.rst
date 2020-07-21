Using the Client API
====================

.. highlight:: python

Advanced Job Submissions
------------------------

You can fine-tune many different aspects of your job submission. Just some
examples include tweaking how many CPUs you'd like to request, how much memory,
setting environment variables, etc.

The following example submits a job to Cook which will use Node.js to print a
message. It requests 512MB of RAM, while also using the ``node:lts`` container
to provide the ``node`` binary.

::

    from cookclient import JobClient
    from cookclient.containers import DockerContainer

    COOK_URL = 'http://localhost:12321'

    client = JobClient(COOK_URL)
    client.submit(command='node -e "console.log(process.env.MESSAGE)"',
                  mem=512,
                  env={'MESSAGE': 'Hello world!'},
                  container=DockerContainer('node:lts'))

Querying Job Status
-------------------

The client library can also be used to query the status of a running job on
Cook. The following example shows one method of waiting until a job has
finished running on Cook, and then printing its state.

::

    import time

    from cookclient import JobClient
    from cookclient.jobs import Status as JobStatus

    COOK_URL = 'http://localhost:12321'

    client = JobClient(COOK_URL)
    job = client.query('123e4567-e89b-12d3-a456-426614174000')
    while job.status != JobStatus.COMPLETED:
        time.sleep(15)
        job = client.query('123e4567-e89b-12d3-a456-426614174000')

    print(job.state)

Killing a Job
-------------

Finally, the client supports killing a job on Cook. The following code snippet
shows how.

::

    from cookclient import JobClient

    COOK_URL = 'http://localhost:12321'

    client = JobClient(COOK_URL)
    client.kill('123e4567-e89b-12d3-a456-426614174000')

.. py:module:: cookclient

Bulk Operations
---------------

In addition to the aforementioned single-job operations, the client also
supports bulk operations. These are bulk submit, bulk query, and bulk kill,
accessible through the :py:meth:`JobClient.submit_all`,
:py:meth:`JobClient.query_all`, and :py:meth:`JobClient.kill_all` methods.
The following is an example of a workflow using these bulk methods:

::

    from cookclient import JobClient

    COOK_URL = 'http://localhost:12321'

    client = JobClient(COOK_URL)
    job1 = {'command': 'ls'}
    job2 = {
        'command': 'echo "Hello World!"',
        'mem': 256.0
    }
    uuids = client.submit_all([job1, job2])
    jobs = client.query_all(uuids)
    for job in jobs:
        print(job)

    client.kill_all(jobs)


Using a Requests Session Object
-------------------------------

``JobClient`` supports receiving a ``requests.Session`` object in its
constructor. If provided, then the provided session will be used to make
requests to Cook. This can be helpful if, for example, you want to use a
specific type of authentication.

``JobClient`` also integrates with Python's ``with`` statement so that the
session is automatically closed once the ``with`` block finishes.

::

    from cookclient import JobClient
    from requests import Session
    from requests_kerberos import HTTPKerberosAuth

    # Change this to match your Cook instance's URL
    COOK_URL = 'https://localhost:12321'

    s = Session(auth=HTTPKerberosAuth())
    with JobClient(COOK_URL, session=s) as client:
        client.submit(command='ls')

Handling Errors
---------------

``JobClient`` will not handle request errors for you. If you want to handle
errors, you must wrap requests in a ``try`` block like so:

::

    from cookclient import JobClient

    COOK_URL = 'http://localhost:12321'

    client = JobClient(COOK_URL)

    try:
        client.submit(command='ls')
    except Exception as e:
        print("Uh-oh!", e)

JobClient Reference
-------------------

.. autoclass:: JobClient
   :members:
