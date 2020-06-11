Using the Client API
====================

The code below shows how to use the client API to connect to a Cook cluster
listening on ``localhost:12321``, submit a job to the cluster, and query its
information.

.. highlight:: python

::

    from cookclient import JobClient

    client = JobClient('localhost:12321')

    uuid = client.submit(command='ls')
    job = client.query(uuid)
    print(str(job))

JobClient Reference
-------------------

.. py:module:: cookclient
.. autoclass:: JobClient
   :members:
