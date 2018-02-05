## What is the difference between share and quota?

Share encapsulates two concepts, the non-preemptable portion of the cluster per user as well as the weight to use when deciding what is a "fair" allocation.

Quota is the maximum resources or count of jobs Cook will allocate to a given user.
It is important to note that reducing quota below what a user is currently running will *not* result in those resources being reclaimed. 
Cook will simply not schedule more jobs for that user until they are below their new quota. 

We recommend using share to ensure users get a percent of their workload shielded from preemption and to weight workloads that are more important (i.e. a production user account).
We recommend keeping quota undefined (i.e. no cap) unless circumstances arise where an operator needs to limit what is running.
For example, limit per user connections to a shared data base or a graceful way to go into maintenance mode. 

## Why does the client generate the job uuid?

Having the client generate the job uuid and use it as part of the submission allows job submission to be idempotent. 
The first submission will succeed and all subsequent submissions with the same uuid will fail. 
This is very powerful as it makes many failure scenarios very easy to handle; 
namely, if the client is unsure if the job was submitted, submit again.

Additionally, since the client knows what the id of the job is before submission, it can store the *intent* to submit before submitting
which makes client side failure easier. Thus, if the client fails after storing intent but before storing submission successful, it
can simply query cook with the intended job uuid to see if it exists

## How can I configure my job to run exactly once?

Cook has the concept of "mea-culpa" retries, for instance when a job is killed due to pre-emption. Some users may prefer for jobs to
just fail in this case instead of attempting to restart it (e.g. when a job is not idempotent.) To prevent this behavior and ensure
the job is only launched once, set `max-retries: 1` and `disable-mea-culpa-retries: true`.

## Does Cook set any environment variables before running a task?

The following environment variables are set by Cook before running a task:
- `COOK_JOB_UUID`: represents the UUID of the job the task belongs to.
- `COOK_JOB_GROUP_UUID`: represents the UUID of the job group the task belongs to.
- `COOK_JOB_CPUS`: represents the amount of `cpus` configured in the job. It can be a fractional number.
- `COOK_JOB_GPUS`: represents the amount of `gpus` configured in the job. It can be a fractional number.
- `COOK_JOB_MEM_MB`: represents the amount of `mem` (in megabytes) configured in the job. It can be a fractional number.

Any environment variables configured in the Job are also included.
In addition, when running an instance using the Cook Executor, the [executor configurations](configuration.adoc#cook_executor) are passed as environment variables.

