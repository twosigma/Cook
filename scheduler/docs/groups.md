# Groups

Groups allows a user to specify that a collections of jobs are related and exposes functionality
that only makes sense for a group.

At its most basic, grouping jobs together allows the ability to query the members of the group.
This is helpful when you care more about the aggregate status of the group and less so about
individual jobs.

Groups also allow the user to tell Cook how to handle the jobs as a group.  For example, the user
may want the jobs to be placed on different machines so they don't interfere or the user may want
any stragglers in the group to be restarted to prevent bad long tail performance.  The former is
referred to as host placement and the latter and straggler handling.  Host placement is coming soon.

## How to make a group

If you just want your jobs related so that you can query them as a group, just add the field `group`
to your job spec and set it to a random uuid (but the same uuid for all the jobs in the group).
Cook will implicitly create a group with that uuid and default settings so that you may query with
it.

If you want to configure some of the group functionality, you can create the group explicitly while
creating the jobs.  When submitting to the rawscheduler, add a `groups` key at the top level and
then specify your groups.  See the api docs for more details.

Currently a job may only be a member of one group, though in the future we plan to lift this
restriction.  For this reason, when you query a job, you will see it has a `groups` field that is a
list.

The groups a job is a member of is immutable and set when you create the job. This is important
because otherwise it would be possible to contradict some of the group scheduler functionality by
adding a group while the job is running.

## How to query a group

There is a group endpoint that can be used to query the configuration, members and status of the
group.  See the api docs for more details.

## Straggler handling

When running a lot of jobs, occasionally it happens that one or two of them run much slower than the
rest.  This can be because they are on a box that is in a bad state, experiencing network problems
or the process is in a bad state. To avoid having to manually monitor the jobs to find and address
these problem cases, Cook can be configured to kill and restart jobs in a group that have been
running much longer then the other jobs in the group. 

To configure this, set the straggler handling type to `quantile-deviation`. This will have Cook look
for jobs that have been running multiple times longer than a particular quantile of the group. The
parameters, `quantile` and `multiplier` decide when a job is considered a straggler. The
`quantile` decides the target runtime of the group and the `multipier` decides how many times
longer than the run time of the quantile job before a job is marked as a straggler.

## Host placement constraints

Sometimes, it is desirable to set some rules on the host distribution to be used for the jobs in a
group.  For example, jobs in a group might run faster if they are all put on different hosts.
Another set of jobs may benefit from running on hosts that share the same attribute, such as
availability zone. Job groups can be configured with host placement constraints to fulfill this
requirement. Currently, there are three types of host placement constraints:

### Unique host constraint
Setting this constraint forces the scheduler to place each job in the group on a different machine
(based on hostname).
Example:
```
{"groups": [{"uuid": "410198dd-c171-470e-aeb8-6ffa4e6a2ada",
            "host-placement": {"type": "unique"}}],
 "jobs": [{"uuid": "831836ee-d5c3-47f7-a052-f4c3ce502495",
           "group": "410198dd-c171-470e-aeb8-6ffa4e6a2ada", ...},
           ...]}}
```


### Balanced host attribute constraint
This constraint takes two parameters: an attribute name and a minimum spread. When using it, the
distribution of hosts will be evenly distributed across values for the named attribute, and the
minimum size of the distribution will be equal to the minimum spread parameter. For example, given
"HOSTNAME" as an attribute name and 3 as the minimum spread, jobs will be distributed to a minimum
of 3 different hosts. If there are 9 jobs in the group, no host may have more than 3 (9 divided by
3) of the jobs on it. However, the jobs may spread to more than 3 hosts: for example, each job could
be on a different host.
Example:
```
{"groups": [{"uuid": "410198dd-c171-470e-aeb8-6ffa4e6a2ada",
            "host-placement": {"type": "balanced",
                               "parameters": {"attribute": "HOSTNAME",
                                              "minimum": 10}}}],
 "jobs": [{"uuid": "831836ee-d5c3-47f7-a052-f4c3ce502495",
           "group": "410198dd-c171-470e-aeb8-6ffa4e6a2ada", ...},
           ...]}
```

### Attribute equals constraint
This constraint takes a single parameter: an attribute name. With this constraint, all jobs in a
group will run in hosts that share the same value for the named attribute. For example, if the
attribute picked is availability zone, all jobs in the group will be run in hosts that share the
availability zone attribute attribute value, such as 'us-east-1b'.
Example:
```
{"groups": [{"uuid": "410198dd-c171-470e-aeb8-6ffa4e6a2ada",
            "host-placement": {"type": "attribute-equals",
                               "parameters": {"attribute": "HOSTNAME"}}}],
 "jobs": [{"uuid": "831836ee-d5c3-47f7-a052-f4c3ce502495",
           "group": "410198dd-c171-470e-aeb8-6ffa4e6a2ada", ...},
           ...]}
```
