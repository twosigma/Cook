# Change log
 
All notable changes to this project will be documented in this file
 
The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](http://semver.org/).

## [1.42.4] - 2020-06-15
### Added
- Validation for GPU model requests, from @kathryn-zhou
### Changed
- Added extra metrics and logging around match cycle, from @scrosby
### Fixed
- Fixed O(#pods * #nodes) bug in calculating k8s offers, from @scrosby

## [1.42.3] - 2020-06-02
### Changed
- Update checkpointing settings, from @nsinkov

## [1.42.2] - 2020-05-26
### Added
- Automates GKE dev environment setup, from @dposada
### Changed
- Don't set cpu limit on sidecar if not setting on main container, from @nsinkov
- Upgrades k8s client library to 7.0.0, from @dposada
- Log pod metadata, from @nsinkov
- Skips match when there are no considerable jobs, from @dposada
### Fixed
- Do not use :missing state for preempted pod, from @nsinkov

## [1.42.1] - 2020-05-12
### Changed
- Added check for k8s node preemption using preemption pod label, from @nsinkov
### Fixed
- Resolved `ClassNotFoundException` for Mesos task-launching, from @dposada

## [1.42.0] - 2020-05-08
### Added
- `Killed by user` reason code, from @nsinkov
- Fallback to k8s checkpointing disabled when max attempts exceeded, from @nsinkov
- Logging of k8s pod events, from @dposada
### Changed
- Added `safe-to-evict` annotation to k8s synthetic pods, from @dposada
- Made matches go to `launch-tasks` in bulk, from @dposada

## [1.41.0] - 2020-04-30
### Added
- Support for specifying the default container on a per-pool basis, from @scrosby
- Add memory overhead accounting when checkpointing, from @nsinkov
- Add lock-sharding to k8s controller, from @dposada
- Add MESOS_DIRECTORY to the k8s environment, from @dposada
### Changed
- Launches k8s tasks in parallel, from @dposada
- Add flag to use google service account for authentication, from @nsinkov
- Add ability to use google metadata server for authentication, from @nsinkov
### Fixed
- Bring all config.edn files up to date, from @scrosby
- Help-make-cluster script uses now unavailable gke k8s version, from @scrosby
- Stop writing synthetic pod info to datomic, from @dposada

## [1.40.4] - 2020-04-24
### Added
- Improved k8s autoscaling metrics, from @dposada
- Allow removing cpu limit in k8s, from @dposada
- Show rate limited users in HTTP log, from @scrosby
- Change checkpointing volume from init container to scratch space, from @nsinkov

## [1.40.3] - 2020-04-10
### Added
- Main container environment variables to init container in k8s, from @nsinkov
- Writable scratch space separate from the k8s sandbox, from @nsinkov
- Experimental API and schema support for checkpointing in k8s, from @nsinkov
- Fast fail for k8s pods with un-initialized containers, from @dposada
### Fixed
- Made k8s sidecar readiness probe optional, from @DaoWen
- Made k8s pod watch initialization process each pod only once, from @dposada
- Removed node anti-affinity for blocklist labels from k8s synthetic pods, from @dposada

## [1.40.2] - 2020-03-24
### Fixed
- Added node anti-affinity for blocklist labels to synthetic pods, from @dposada

## [1.40.1] - 2020-03-22
### Added
- Mesos sandbox mount to k8s pods (backward compatibility for jobs that assume they're running on Mesos), from @nsinkov

## [1.40.0] - 2020-03-19
### Added
- Progress reporting for k8s jobs, from @DaoWen
- SSL verification between Cook and k8s, from @scrosby
- Fast fail for unschedulable k8s pods, from @dposada
- Support for k8s synthetic pod namespace to be user's namespace, from @dposada
- k8s synthetic pod anti-affinity to previous hosts, from @dposada 
### Changed
- Made autoscaling (for k8s) based on pending jobs instead of match failures, from @dposada
- Renamed k8s metrics to be consistent with prior metric naming, from @scrosby
- Separated k8s job pods' workdir and sandbox, from @DaoWen

## [1.39.1] - 2020-03-03
### Fixed
- Removed expensive log from `handle-resource-offers!`, from @dposada

## [1.39.0] - 2020-03-03
### Added
- Mapping for Mesos reason `REASON_TASK_KILLED_DURING_LAUNCH`, from @dposada
- Experimental support for synthetic k8s pods to trigger the cluster autoscaler, from @dposada
- Metrics to k8s code, from @scrosby
### Changed
- Made "Container launch failed" mea culpa, from @dposada
### Fixed
- NPE in sandbox calculation when compute cluster is not found, from @scrosby

## [1.38.4] - 2020-02-13
### Changed
- Integration test improvements, from @dposada and @scrosby

## [1.38.3] - 2020-02-13
### Added
- Support to blacklist nodes in k8s that have certain labels, from @scrosby
### Fixed
- Bug in reading default pool from config when using k8s sidecar, from @nsinkov
- Bug in job progress aggregation, from @DaoWen
- Handling of node preemption, from @dposada and @scrosby
- Handling of k8s startup connection errors, from @scrosby
- Handling of bad request response from k8s, from @scrosby 
- Handling when a running pod goes completely missing, from @scrosby

## [1.38.2] - 2020-01-31
### Added
- REST endpoint for posting job progress updates, from @DaoWen
### Fixed
- Bug in k8s state machine for completed instances, from @scrosby
- Bug in k8s pod resource requests, from @DaoWen
- Handling of pod submission failures, from @dposada
- Race where Cook can kill a task then later launch it, from @scrosby

## [1.38.1] - 2020-01-26
### Changed
- Improved logging for k8s compute clusters, from @dposada

## [1.38.0] - 2020-01-24
### Added
- Logs fileserver for k8s jobs, from @nsinkov
- Missing state pairs in the k8s controller, from @scrosby
### Fixed
- Default the user parameter in docker, from @shamsimam
- Improve k8s node and pod watches so that they retry forever, from @scrosby
- Correct misnumbered 403 error codes for Swagger, from @DaoWen

## [1.37.0] - 2020-01-15
### Added
- Support for moving a portion of a user's jobs to a different pool, from @dposada
- Support in k8s compute clusters for max pods per node, from @dposada
### Fixed
- Made Mesos reconciler only reconcile Mesos tasks, from @scrosby
- Made declining Mesos offers work, from @shamsimam
- Removed incorrect rate-limit reason in `/unscheduled_jobs`, from @dposada

## [1.36.2] - 2019-12-17
### Fixed
- Avoid using Cook executor when launching on k8s, from @dposada
### Changed
- Made container defaults be compute-cluster specific, from @dposada
- Added mapping for the Failed pod phase on k8s, from @dposada

## [1.36.1] - 2019-12-05
### Changed
- Reverted a change that added unexpectedly expensive logging, from @scrosby

## [1.36.0] - 2019-12-04
### Added
- Support for multiple kubernetes compute clusters, from @scrosby
- Support for mesos and kubernetes compute clusters simultaneously, from @scrosby
- Scripts for creating compute clusters on GKE, from @scrosby
### Changed
- Optimized quota reading, from @shamsimam
- Integration test improvements, from @dposada
- Bug fixes for kubernetes support, from @scrosby

## [1.35.0] - 2019-10-21
### Added
- Max ports to task constraints, from @pschorf
- Leader URL to `/info`, from @dposada
### Changed
- Max priority to 16,000,000, from @nsinkov

## [1.34.2] - 2019-10-02
### Changed
- Integration test improvements, from @dposada

## [1.34.1] - 2019-09-24
### Added
- Pool name to matching logs, from @dposada

## [1.34.0] - 2019-09-11
### Added
- `COOK_INSTANCE_NUM` environment variable, from @pschorf
- Metrics on instance fetch rates, from @scrosby
- Capturing the time it takes to list jobs, from @scrosby

## [1.33.0] - 2019-08-07
### Added
- Support for multiple submit plugins, from @pschorf
- Maximum command line length parameter, from @pschorf
- Improved error logging, from @pschorf
### Changed
- Check quota when rebalancing, from @pschorf
- Optimize the job fetching code to not round-trip to UUID, from @scrosby

## [1.32.1] - 2019-06-20
### Fixed
- Fixed rebalancer bug, from @pschorf

## [1.32.0] - 2019-06-19
### Added
- Support for default container volumes, from @pschorf
### Changed
- Limit number of jobs eligible for matching for out of quota users, from @pschorf

## [1.31.0] - 2019-05-09
### Added
- Compute cluster on task objects, from @scrosby
- Support running Cook Executor in docker containers, from @pschorf
### Changed
- Filter jobs that would put users out of quota from /queue endpoint, from @pschorf

## [1.30.1] - 2019-04-30
### Fixed
- Fixed bug in user metric reporting, from @pschorf

## [1.30.0] - 2019-04-23
### Added
- Added support for file_url, from @pschorf
### Fixed
- Fix for periodic job cleanup, from @scrosby

## [1.29.0] - 2019-03-19
### Added
- Instance completion plugin, from @pschorf
- Periodic cleanup of uncommitted jobs, from @scrosby
- Pool selection plugin, from @pschorf

## [1.28.0] - 2019-03-05
### Added
- Added support for suitable flag for datasets, from @pschorf
- Added plugin support for job submission and launch, from @scrosby
- Added COOK_INSTANCE_UUID to task environment, from @dposada
### Changed
- Allow setting cook executor retry limit to 0, from @pschorf

## [1.27.0] - 2019-01-23
### Added
- Support for docker images in mesos containerizer, from @pschorf

## [1.26.0] - 2018-12-11
### Added
- Global launch rate limit, from @scrosby
### Changed
- Made per-user rate limit more gradual, from @scrosby
- Sped up `/unscheduled` endpoint with new query and truncating long lists, from @pschorf

## [1.25.0] - 2018-11-08
### Added
- Support for job launch rate limits, from @scrosby
### Changed
- Updated dependencies for integration tests to newer versions

## [1.24.0] - 2018-10-26
### Added
- Support for x-cook-pool header, from @pschorf
### Fixed
- Bug in reporting total usage when pools are enabled, from @pschorf

## [1.23.0] - 2018-10-11
### Changed
- Updated some metric names to incorporate pools, from @pschorf and @dposada

## [1.22.0] - 2018-10-04
### Added
- Rate limiting on job submission, from @scrosby
### Fixed
- Remove stale dataset cost data, from @pschorf
- Don't show uncommitted jobs in unscheduled_jobs endpoint, from @pschorf

## [1.21.0] - 2018-09-12
### Added
- Support for contacting a data local service to obtain cost data for scheduling, from @pschorf
### Fixed
- Bug in quota-checking when running without pools, from @dposada
- Bug in the rebalancer's retrieval of DRU divisors when running with pools, from @dposada
- Integer overflows in timer tasks when the scheduler runs for a long time, from @shamsimam

## [1.20.0] - 2018-09-05
### Added
- Per-pool job scheduling, from @dposada and @pschorf
- Support for self-impersonation requests from normal users, from @DaoWen
- Exit code syncer to handle a high rate of incoming exit code messages, from @shamsimam
### Changed
- Removed TTL from agent attributes cache, from @dposada

## [1.19.1] - 2018-07-30
### Changed
- Performance improvements to job submission, from @scrosby and @pschorf

## [1.19.0] - 2018-07-26
### Added
- data-local field to jobs, from @pschorf
### Changed
- Performance improvements to job submission, from @scrosby and @pschorf

## [1.18.1] - 2018-07-03
### Fixed
- Consume entire request before sending response, from @pschorf

## [1.18.0] - 2018-06-25
### Added
- Container fields to /jobs, from @dposada
- reason_mea_culpa to instance responses, from @dposada
- Support for x-forwarded-proto header for CORS requests, from @pschorf
### Changed
- Removed mesos master-hosts config, from @dposada
- Removed rebalancer min-utilization-threshold, from @dposada

## [1.17.3] - 2018-06-14
### Changed
- Better authorization failed message on job deletion, from @dposada
### Fixed
- Handle edge case in estimated completion constraint, from @pschorf

## [1.17.2] - 2018-06-01
### Fixed
- Issue where task reconciliation was failing, from @pschorf

## [1.17.1] - 2018-05-23
### Fixed
- Issue where nil instance timestamps would cause NPEs, from @dposada

## [1.17.0] - 2018-05-22
### Added
- Pool support to /jobs, from @dposada
- Estimated completion constraint, from @pschorf
- Pool submap to /quota and /share, from @pschorf
### Changed
- Improvements to job query times, from @scrosby

## [1.16.0] - 2018-05-08
### Added
- Added pool support to /share and /quota endpoints, from @pschorf
### Changed
- Returns 409 on some retry operations instead of retrying jobs which could end up in a bad state, from @pschorf
- Fixed bug with disable_mea_culpa_retries, from @pschorf
- Improved logging for some error cases, from @dposada


## [1.15.0] - 2018-04-17
### Added
- Support for pool param to /usage endpoint, from @dposada
- Support for pool param on job submission, from @dposada
- Support for SSL, from @pschorf
- Support for api-only mode, from @dposada
### Fixed
- Issue where monitor metrics would sometimes stop on a non-zero value, from @dposada

## [1.14.1] - 2018-03-29
### Fixed
- Fix performance regression in list API, from @scrosby

## [1.14.0] - 2018-03-28
### Added
- Support for listing custom executor jobs in /jobs endpoint, from @dposada
- Kill instances for cancelled jobs on leadership election, from @pschorf
### Changed
- Performance improvements to scheduling and list APIs, from @scrosby
### Fixed
- Fixed GPU support, from @dPeS

## [1.13.0] - 2018-03-13
### Added
- Support for CORS requests, from @pschorf
- Scheduling performance improvements, from @scrosby
- Counters for job cpu/mem/runtime by failure reason, from @dposada 

## [1.12.0] - 2018-03-05
### Added
- Endpoint for instance statistics, from @dposada
- Support for a configurable run as user, from @shamsimam
- Support for configuring number of instances which can fail before falling back to the mesos executor, from @shamsimam
### Changed
- Performance improvements to sandbox syncer, from @shamsimam
- Rebalancer now reserve hosts after preempting, from @pschorf
- Performance improvents to dru computation, @shamsimam

## [1.11.0] - 2018-02-05
### Added
- Added timely sandbox directory updates for tasks that are not executed by the cook executor, from @shamsimam
- Added environment variables that contain the resources requested by the job, from @shamsimam
### Changed
- Converted monitor Riemann events to codahale metrics, from @dposada
### Fixed
- Fixed string encoding on `/rawscheduler` POST, from @pschorf
- The `start-time` timestamp on `/info` no longer re-evaluates to `now` on each request, from @DaoWen

## [1.10.0] - 2018-01-22
### Added
- Added user-impersonation functionality to support services running on top of Cook Scheduler, from @DaoWen
### Changed
- Jobs that exceed a user's total resource quota are rejected rather than waiting indefinitely, from @DaoWen

## [1.9.0] - 2018-01-10
### Added
- Added unauthenticated /info endpoint for retrieving basic setup information, from @DaoWen
- Added metrics for message rates of Mesos status changes and framework updates, from @shamsimam
- Added check for required `reason` parameter on share and quota deletions, from @DaoWen
### Changed
- Fixed error in Kerberos middleware setup, from @DaoWen
- Reclassified `MESOS_EXECUTOR_TERMINATED` as a mea-culpa error, from @shamsimam
- Fixed bug preventing group retry updates by non-admin users, from @DaoWen
- Fixed bug causing a 500 rather than a 404 for gets on non-existent groups, from @DaoWen
- Re-enabled Fenzo group constraints, from @pschorf

## [1.8.3] - 2017-12-12
### Added
- Added /instances endpoint for retrieving job instances, from @dposada
- Added /jobs resource for retrieving jobs, from @dposada
- Added /usage endpoint for displaying user resource usage, from @DaoWen
- Added failed-only option for retry endpoint, from @DaoWen
### Changed
- Fixed authorization check on group endpoint, from @DaoWen
- Disabled fenzo group constraints, from @pschorf

## [1.8.2] - 2017-11-13
### Changed
- Retries sandbox syncing of hosts when cache entries expire, from @shamsimam
- Allow partial results from /unscheduled_jobs, from @dposada
- Improve performance by defering calculation of group components, from @pschorf
- Support millisecond time resolution for lingering tasks, from @DaoWen

## [1.8.1] - 2017-11-08
### Added
- Added COOK_JOB_UUID and COOK_JOB_GROUP_UUID to the job environment, from @shamsimam

## [1.8.0] - 2017-11-07
### Added
- Added support for killing a group of jobs, from @DaoWen
- Added sysouts to get job output closer to Mesos' CommandExecutor, from @shamsimam
- Added metrics for usage of /list, from @dposada
- Added support for retrying a group of jobs, from @DaoWen
- Added support for configurable environment passed to Cook Executor, from @shamsimam
### Changed
- Fixed bug with job group constraints, from @pschorf
- Fixed bug where Cook Executor jobs were opting in to the heartbeat support, from @shamsimam
- Changed (simplified) the sandbox directory syncing mechanism for jobs, from @shamsimam

## [1.7.0] - 2017-10-19
### Changed
- Renamed user whitelist to users allowed, from @dposada
- Fixes for stderr/out file handling in Cook executor, from @shamsimam
- Fixed bug with /unscheduled_jobs endpoint, from @pschorf

## [1.6.0] - 2017-09-22
### Added
- Added support for allowing job to specify which executor (cook|mesos) to use, from @shamsimam
- Added support for passing state=success/failed in /list, from @dposada
- Added support for filtering by name in /list, from @dposada
### Changed
- More failure codes have been classified as mea-culpa failures, from @pschorf
- /queue endpoint redirects to the master on non-master hosts, from @pschorf
- Fixed handling of detailed parameter on group queries, from @DaoWen
- Fixed bug with launching docker container jobs, from @DaoWen
- Fixed bug with docker container port mappings, from @pschorf

## [1.5.6] - 2017-08-07
### Changed
- Performance improvement in rank jobs, from @wyegelwel

## [1.5.5] - 2017-08-04
### Added
- Added JVM metric reporting, from @pschorf
- Added support for partial results when querying for groups, from @dposada
- Added support for user whitelisting, from @dposada

## [1.5.4] - 2017-07-31
### Added
- Added support for throttling rate of publishing instance progress updates, from @shamsimam
- Added authorization check for job creation, from @dposada

## [1.5.3] - 2017-07-17
### Added
- The Mesos Framework ID is now configurable, from @dposada
- Added configuration for agent-query-cache, from @shamsimam
- Added support for Cook Executor, from @shamsimam
### Changed
- Replaced aggregate preemption logging with individual preemption decisions, from @wyegelwel
- /debug endpoint now returns the version number, from @dposada

## [1.5.2] - 2017-06-23
### Fixed
- Fixed a bug which was overwriting end-time on duplicate mesos messages, from @pschorf
- Fixed a bug with querying for jobs with a non-zero number of ports, from @dposada

## [1.5.1] - 2017-06-22
### Changed
- Parallelize in-order processing of status messages, from @shamsimam
- Change reason string from "Mesos command executor failed" to "Command exited non-zero", from @wyegelwe

## [1.5.0] - 2017-06-21
### Added
- Added configuration option for the leader to report unhealthy, from @pschorf
### Changed
- Optimized list endpoint query for running and waiting jobs, from @wyegelwel and @pschorf

## [1.4.2] - 2017-06-20
### Changed
- Lowered log level of sandbox directory fetch error to reduce noise, from @wyegelwel
- Further optimize list endpoint query, from @pschorf and @wyegelwel

## [1.4.1] - 2017-06-20
### Changed
- Optimized the query in the list endpoint to avoid an expensive datomic join, from @pschorf and @wyegelwel
- Change the list endpoint time range to be inclusive on start, from @wyegelwel
- Add check to ensure job/group uuids do not exist before creation, from @pschorf
- Limit rebalancer jobs to consider to max preemptions, from @wyegelwel

## [1.4.0] - 2017-06-09
### Added
- Added simulator to test scheduler performance, from @wyegelwel
- Added job constraints, from @wyegelwel
- Added instance progress to query response, from @dposada
### Changed
- Fixed bug where job submit errors would return 201, from @pschorf
- Optimizations in ranking to improve schedule time, from @shamsimam
- Refactor fenzo constraints to use less memory, from @pschorf
## [1.3.2] - 2017-05-24
### Added
- Added disable-mea-culpa-retries to jobclient, from @WenboZhao
### Changed
- Fix bug with disable-mea-culpa-retries, from @pschorf
## [1.3.1] - 2017-05-18
### Changed
- Make DRU order deterministic, from @wyegelwel
- Change default cycle time for checking max-runtime exceeded to 1m, from @wyegelwel
- Remove concat usage, from @pschorf
## [1.3.0] - 2017-05-05
### Added
- /unscheduled_jobs API endpoint, from @mforsyth
- Added application to job description, from @dposada
- Added disable-mea-culpa-retries flag, from @pschorf
- Added docker, from @dposada
- Added support for job groups in simulator, from @mforsyth
- Added /failure_reasons API endpoint, from @mforsyth
- Added expected-runtime to job description, from @dposada
- Added /settings API endpoint, from @dposada
- Added group host placement constraints, from @DiegoAlbertoTorres
### Changed
- Require an explicit reason when changing shares or quotas (from @mforsyth).  This intentionally breaks backwards compatibility.
- Optimized matching code to speed schedule time @wyegelwel
- Stream JSON responses, from @pschorf
- Speed up ranking with commit latch and caching from @wyegelwel

## [1.2.1] - 2017-03-13
### Fixed
- Fixed a bug with calculating whether we matched the head of the queue which caused cook to only schedule 1 job at a time. (this is why 1.2.0 was yanked)

## [1.2.0] - 2017-03-13 - [YANKED]
### Added
- Start of CHANGELOG. We are likely missing some items from 1.0.1, will be better from now on.
- Switch to use Fenzo for matching from @dgrnbrg and @mforsyth
- GPU support from @dgrnbrg
- Swaggerized endpoints from @mforsyth
- Groups (https://github.com/twosigma/Cook/blob/master/scheduler/docs/groups.md) from @DiegoAlbertoTorres
- Containers support from @sdegler, @leifwalsh, @wyegelwel
- Retry endpoint from  @pjlegato and @wyegelwel
- Authorization on endpoints from @pjlegato and @wyegelwel
- System simulator and CI from @mforsyth
- Access logs for server from @sophaskins
- Mea culpa reasons so some failures don't count against retries from @DiegoAlbertoTorres @mforsyth
 
### Changed
- Switch to use [mesomatic](https://github.com/pyr/mesomatic) over clj-mesos from @mforsyth
- Tied to mesos 1.x.x (exact version is 1.0.1)
- State change of a job from waiting to running now occurs when Cook submits the job to mesos (not when mesos confirms the job is running) from @aadamson and @DiegoAlbertoTorres
- Performance improvements to ranking and scheduling from @wyegelwel
 
### Fixed
- Split brain on mesos / zk fail over. Cook will now exit when it loses leadership with either zk or mesos. A supervisor is expected to restart it from@wyegelwel
