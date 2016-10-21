## What is the difference between share and quota

Share encapsulates two concepts, the non-preemptable portion of the cluster per user as well as the weight to use when deciding what is a "fair" allocation.

Quota is the maximum resources or count of jobs Cook will allocate to a given user.
It is important to note that reducing quota below what a user is currently running will *not* result in those resources being reclaimed. 
Cook will simply not schedule more jobs for that user until they are below their new quota. 

We recommend using share to ensure users get a percent of their workload shielded from preemption and to weight workloads that are more important (i.e. a production user account).
We recommend keeping quota undefined (i.e. no cap) unless circumstances arise where an operator needs to limit what is running.
For example, limit per user connections to a shared data base or a graceful way to go into maintenance mode. 
