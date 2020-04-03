# Named constants for failure reason codes from cook or mesos.
# See scheduler/src/cook/mesos/schema.clj for the reason code names.
REASON_TASK_KILLED_DURING_LAUNCH = 1004
MAX_RUNTIME_EXCEEDED = 2003
CONTAINER_INITIALIZATION_TIMED_OUT = 1007
EXECUTOR_UNREGISTERED = 6002
UNKNOWN_MESOS_REASON = 99001
CMD_NON_ZERO_EXIT = 99003

# Named constants for unscheduled job reason strings from cook or fenzo.
UNDER_INVESTIGATION = 'The job is now under investigation. Check back in a minute for more details!'
COULD_NOT_PLACE_JOB = 'The job couldn\'t be placed on any available hosts.'
JOB_WOULD_EXCEED_QUOTA = 'The job would cause you to exceed resource quotas.'
JOB_IS_RUNNING_NOW = 'The job is running now.'
JOB_LAUNCH_RATE_LIMIT = 'You are currently rate limited on how many jobs you launch per minute.'
PLUGIN_IS_BLOCKING = 'The launch filter plugin is blocking the job launch.'
