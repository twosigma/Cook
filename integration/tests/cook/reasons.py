# Named constants for failure reason codes from cook or mesos.
# See scheduler/src/cook/mesos/schema.clj for the reason code names.
MAX_RUNTIME_EXCEEDED = 2003
EXECUTOR_UNREGISTERED = 6002
UNKNOWN_MESOS_REASON = 99001
CMD_NON_ZERO_EXIT = 99003

# Named constants for unscheduled job reason strings from cook or fenzo.
UNDER_INVESTIGATION = 'The job is now under investigation. Check back in a minute for more details!'
COULD_NOT_PLACE_JOB = 'The job couldn\'t be placed on any available hosts.'
JOB_WOULD_EXCEED_QUOTA = 'The job would cause you to exceed resource quotas.'
JOB_IS_RUNNING_NOW = 'The job is running now.'
JOB_LAUNCH_RATE_LIMIT = 'You have exceeded the limit of jobs launched per minute.'
