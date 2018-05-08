# Change log
 
All notable changes to this project will be documented in this file
 
The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](http://semver.org/).

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
