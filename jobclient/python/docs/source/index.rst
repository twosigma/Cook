.. Cook Python Client API documentation master file, created by
   sphinx-quickstart on Mon Jun  8 10:47:38 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Cook Python Client API
======================

This is the official Python client library for interacting with Cook Scheduler.

Quickstart
----------

The code below shows how to use the client API to connect to a Cook cluster
listening on http://localhost:12321, submit a job to the cluster, and query its
information.

.. highlight:: python

::

    from cookclient import JobClient

    client = JobClient('localhost:12321')

    uuid = client.submit(command='ls')
    job = client.query(uuid)
    print(str(job))



.. toctree::
   :maxdepth: 2
   :caption: Contents:

   usage
   api



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
