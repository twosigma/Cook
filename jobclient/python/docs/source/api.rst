API Reference
=============

Cook Objects
------------

This section describes the different objects returned by the Cook API.

Jobs
^^^^

The most important Cook object is the Job, which is the object returned via the
``query`` method on the JobClient. It is a simple container class that houses
the attributes returned by the Cook REST API.

.. py:module:: cookclient.jobs

.. autoclass:: Job
   :members:

Job Status and State
^^^^^^^^^^^^^^^^^^^^

Jobs have *status* and *state*, two related but different indicators that
describe whether a job is running or not (its status) and whether that job has
succeeded or not (its state).

.. autoclass:: Status
   :members:

.. autoclass:: State
   :members:

Application Information
^^^^^^^^^^^^^^^^^^^^^^^

Jobs may also have application information attached, which serve to label a
job's origin. By default, the client API will attach its own application
information (``cook-python-client`` with the client's current version), but
this may be overridden in the ``JobClient.submit`` method.

.. autoclass:: Application
   :members:

Instances
^^^^^^^^^

A job may attempt to run several times on a Cook cluster. Each of these
attempts is known as an *instance*, and is represented with the ``Instance``
class:

.. py:module:: cookclient.instance

.. autoclass:: Instance
   :members:

Instance Status
^^^^^^^^^^^^^^^

Instances have their own status information associated with them:

.. autoclass:: Status
   :members:

Executor Information
^^^^^^^^^^^^^^^^^^^^

Instances run on *executors*. The API supports two types of executors,
described below:

.. autoclass:: Executor
   :members:

Utility Functions
-----------------

Timestamp / Datetime Conversion
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Cook API returns Unix millisecond timestamps for timestamp values. To
provide a nicer, higher-level API, the Python client automatically converts
these into Python ``datetime`` objects using the following functions:

.. py:module:: cookclient.util

.. autofunction:: unix_ms_to_datetime

.. autofunction:: datetime_to_unix_ms

Temporal UUID
^^^^^^^^^^^^^

When dealing with job UUIDs, Cook performs much better when UUIDs are clustered
temporally. When submitting a job to the cluster, if no UUID is provided, then
the client API will use this function to generate a random UUID that clusters
temporally:

.. autofunction:: make_temporal_uuid

