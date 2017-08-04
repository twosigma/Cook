# Change log
 
All notable changes to this project will be documented in this file
 
The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](http://semver.org/).

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
