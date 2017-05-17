"""Cook Executor

The Cook executor is a custom executor written in Python.
It replaces the default command executor in order to enable a number of
features for both operators and end users.
For more information on Mesos executors, see the "Working with Executors" 
section at http://mesos.apache.org/documentation/latest/app-framework-development-guide/
"""

TASK_FAILED = 'TASK_FAILED'
TASK_FINISHED = 'TASK_FINISHED'
TASK_KILLED = 'TASK_KILLED'
TASK_RUNNING = 'TASK_RUNNING'

PROGRESS_MESSAGE_KEY = 'progress-message'
