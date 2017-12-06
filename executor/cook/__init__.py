"""Cook Executor

The Cook executor is a custom executor written in Python.
It replaces the default command executor in order to enable a number of
features for both operators and end users.
For more information on Mesos executors, see the "Working with Executors" 
section at http://mesos.apache.org/documentation/latest/app-framework-development-guide/
"""

DAEMON_GRACE_SECS = 1
TERMINATE_GRACE_SECS = 0.1

REASON_CONTAINER_LIMITATION_MEMORY = 'REASON_CONTAINER_LIMITATION_MEMORY'
REASON_EXECUTOR_TERMINATED = 'REASON_EXECUTOR_TERMINATED'
REASON_TASK_INVALID = 'REASON_TASK_INVALID'

TASK_ERROR = 'TASK_ERROR'
TASK_FAILED = 'TASK_FAILED'
TASK_FINISHED = 'TASK_FINISHED'
TASK_KILLED = 'TASK_KILLED'
TASK_RUNNING = 'TASK_RUNNING'
TASK_STARTING = 'TASK_STARTING'
