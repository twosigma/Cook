Cook is a powerful, multitenant batch scheduler focused on providing high utilization and fairness among users when there are more jobs to run than computers to run them on. As a scheduler, Cook must decide what should be running and where the jobs should run.  

# Jobs

A job is the core unit in Cook. A job can be thought of as the intent to run a command on the cluster,
when the job is started on a specific host on the cluster, it is referred to as an instance of the job. Jobs may be started multiple times 
in the event that it fails and so it may have multiple instances. Cook assumes jobs are idempotent and additional, out of band work must 
be done if your job is not.

A job is specified with a bash command, the resource requirements (cpus, mem, gpus), the number of times to retry the job, 
and the priority of the job. Additionally, there are a variety of optional parameters that can be set for a job, including a container to
run it with. The [rest api specification](./scheduler-rest-api.adoc) describes all the options. 

A job may be a member of a [group](./groups.md) of jobs. Grouping jobs allows for additional semantics in querying, retrying, killing, and placing jobs. 

When jobs are submitted to Cook, they are added to the submitting user's personal queue. At this point the job is considered to be in the "waiting" state. Each user's queue is combine to create a central queue which is used to make scheduling decisions. The position of a job in the global queue is based on its position in the user queue as well as the resources the user is already using compared to other users. The job will be matched to a host with the resources to run the job at which point an instance of the job will be sent to mesos to run the job on that host. While an instance of the job is running, the job is in the "running" state. If that instance completes successfully, the job is considered successful. If instead the instance fails, the job will be placed back in the queue with state "waiting" if it has retries remaining otherwise it will be considered failed. 

# What should be running: job ranking and preemption

In deciding what to run, Cook is focused on ensuring fairness among users. Currently, we consider fairness to mean users are allocated equal amounts of resources at any time. Each user's jobs (running and waiting) are sorted such that higher priority, longer running time and earlier submission are more important. Then, Cook generates a global ordering by taking jobs from the user job orderings such that the user with the lowest resources allocated thus far has their job added next. 

Cook uses this ordering for two things:

1. Deciding what waiting job to run next on available resource
2. Deciding what jobs to preempt in favor of more important jobs 

The second item is called preemption and is the job of the rebalancer in Cook. The rebalancer runs periodically and greedily tries to determine which jobs to preempt such that more important jobs may run. Jobs may be preempted so that a user with low utilization may get more resources or a user's higher priority job may run instead of the user's lower priority job.

# Where should the job be running: matching

Matching is the process of taking waiting jobs and deciding what available host to run the on. When matching jobs to available resources, Cook removes jobs that are already running from this global ordering. Cook then tries to place the most important job on the available hosts by first checking it will fit and passes the constraints the job has and second such that it is bin packed well. Whether or not the most important job could be placed, Cook will try the next job with the remaining resources. This will continue until Cook runs out of jobs or resources to consider. If Cook was unable to place the most important job, it will shrink the number of jobs to consider until at last only the most important job is considered until it is scheduled. The fenzo [configurations options](./configuration.adoc) explain this in more detail. 




