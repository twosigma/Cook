# Groups

Groups allows a user to specify that a collections of jobs are related and exposes
functionality that only makes sense for a group.

At its most basic, grouping jobs together allows the ability to query the members of the group.
This is helpful when you care more about the aggregate status of the group and less so about
individual jobs.

Groups also allow the user to tell Cook how to handle the jobs as a group.
For example, the user may want the jobs to be placed on different machines so they don't interfere 
or the user may want any stragglers in the group to be restarted to prevent bad long tail performance. 
The former is referred to as host placement and the latter and straggler handling. 
Host placement is coming soon.

## How to make a group

If you just want your jobs related so that you can query them as a group, just add the field `group`
to your job spec and set it to a random uuid (but the same uuid for all the jobs in the group).
Cook will implicitly create a group with that uuid and default settings so that you may query with it.

If you want to configure some of the group functionality, you can create the group explicitly while creating the jobs.
When submitting to the rawscheduler, add a `groups` key at the top level and then specify your groups. 
See the api docs for more details.

Currently a job may only be a member of one group, though in the future we plan to lift this restriction.
For this reason, when you query a job, you will see it has a `groups` field that is a list.

The groups a job is a member of is immutable and set when you create the job. This is important because 
otherwise it would be possible to contradict some of the group scheduler functionality by adding a group
while the job is running.

## How to query a group

There is a group endpoint that can be used to query the configuration, members and status of the group.
See the api docs for more details.

## Straggler handling

When running a lot of jobs, occasionally it happens that one or two of them run much slower than the rest.
This can be because they are on a box that is in a bad state, experiencing network problems or the process
is in a bad state. To avoid having to manually monitor the jobs to find and address these problem cases,
Cook can be configured to kill and restart jobs in a group that have been running much longer then the
other jobs in the group. 

To configure this, set the straggler handling type to `quantile-deviation`. This will have Cook look
for jobs that have been running multiple times longer than a particular quantile of the group. 
The parameters, `quantile` and `multiplier` decide when a job is considered 
a straggler. The `quantile` decides the target runtime of the group and the `multipier` 
decides how many times longer than the run time of the quantile job before a job is marked as a 
straggler.



  

