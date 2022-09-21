# Change log
 
All notable changes to this project will be documented in this file
 
The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](http://semver.org/).

## [1.63.4] - 2022-09-21
### Fixed
- Fixed bug in parallelized Kubernetes watch processing, from @scrosby
### Changed
- Make prometheus JVM metrics use compute cluster name, from @samincheva

## [1.63.3] - 2022-09-13
### Changed
- Parallelize Kubernetes watch processing, from @scrosby

## [1.63.2] - 2022-09-09
### Fixed
- Do not set scalar-requests to pool specific resources for Kenzo pods, from @ahaysx

## [1.63.1] - 2022-09-07
### Changed
- Reverted parallel Kubernetes watch processing for future release

## [1.63.0] - 2022-09-06
### Added
- Prometheus metrics
  - JVM metrics, from @samincheva
  - Ring metrics, from @samincheva
  - Parity for remaining codahale metrics, from @samincheva
- Direct-to-Kubernetes scheduler (Kenzo)
  - Use backpressure of scheduling pods to moderate launching new pods for real
    jobs, from @ahaysx
  - Prometheus metrics parity, from @ahaysx

### Changed
- Parallelize Kubernetes watch processing, from @scrosby
- Optimize getting the nodename from a node, from @scrosby

## [1.62.6] - 2022-08-26
### Added
- Initial implementation for submitting jobs directly to Kubernetes Scheduler, from @ahaysx
- Better error handling in scheduler/write functions, from @ahaysx
- Launch tasks similarly for both Fenzo and K8s Scheduler pools, from @ahaysx
- Adding prometheus metrics to remaining modules, from @samincheva

### Fixed
- Optimizing total pod count metric and fixing mismatched metric labels, from @samincheva

## [1.62.5] - 2022-08-18
### Added
- Make Fenzo config pool-specific and set up for other schedulers, from @ahaysx
- Prometheus metrics for the kubernetes, API, and tools module, from @samincheva
- Update of synthetic pods counter metric even if the current match cycle doesn't autoscale, from @samincheva

### Changed
- Performance optimization for add-starting-pods, from @scrosby
- Remove metatransaction filter from match, from @scrosby
- Make job resource lookup more efficient in miss path, from @scrosby
- Relazy some list generation in rank cycle, from @scrosby
- Parallelize autoscale to run at the same time as main job launches, from @scrosby

## [1.62.4] - 2022-08-12
### Added
- Add new JobSubmissionModifier and refactor JobRouter, from @laurameng

### Changed
- Prometheus metrics
  - Updated match cycle metric logic for 0 considerable case, from @samincheva
  - Added prometheus metric for synthetic pods count, from @samincheva
- Use a factory fn for creating (future) different types of pool handlers, from @ahaysx

## [1.62.3] - 2022-08-03
### Changed
- Configured the /metrics endpoint to have a separate rate limit, from @samincheva

## [1.62.2] - 2022-08-02
### Added
- Prometheus, from @samincheva
- Adding match cycle metrics to prometheus, from @samincheva
- Adding prometheus metric for jobs launch count, from @samincheva

### Fixed
- Use pools & submit pools in /jobs list endpoint, from @laurameng

## [1.62.1] - 2022-07-27
### Added
- Add support for pool quotas across pools, from @scrosby

## [1.62.0] - 2022-07-20
### Added
- Add support for routing jobs between pools based on constraints, from @scrosby

## [1.61.3] - 2022-07-14
### Fixed
- Forced eval of lazy sequence in tracing span causing performance degradation, from @samincheva

## [1.61.2] - 2022-07-12
### Changed
- Add more opentracing spans to the match cycle, from @samincheva

### Fixed
- Disabled pools integration tests handle 0 quota better, from @samincheva

## [1.61.1] - 2022-07-28
### Added 
- Opentracing for the match cycle logic, from @samincheva

### Fixed 
- Allow preemptions for tasks with unknown status, from @ahaysx
- Fix rebalancer integration test to handle failures better, from @ahaysx

## [1.61.0] - 2022-06-22
### Changed
- Moved the global launch/kill ordering lock to be per compute-cluster, from @laurameng

## [1.60.2] - 2022-06-15
### Fixed
- Fix bug in api-only flag that would fail operations requiring a connection to the leader, from @samincheva

## [1.60.1] - 2022-06-09
### Changed
- Cook now determines which pool a k8s node is in via a label instead of a taint, from @scrosby

### Fixed
- Ascribe NodeAffinity k8s failures to node preemption, from @scrosby

## [1.59.7] - 2022-06-02
### Changed
- Updated instance->user cache to handle fake entities for waiting jobs, to speed up worst-case rank loop performance, by @scrosby

### Fixed
- Updated structured logging utility to handle failed json conversion, from @samincheva
- Updated format-map-for-structured-logging to traverse nested maps instead of flattening them, from @samincheva 

## [1.59.6] - 2022-05-31
### Changed
- Updated scheduler, compute cluster, and kubernetes API code to use structured logging, from @samincheva

## [1.59.5] - 2022-05-10
### Added
- Development documentation for Cook, from @scrosby
- Utility for emitting structured logs, from @samincheva
### Changed
- Updated unit tests for GPU model types, from @scrosby
### Fixed
- Corrected numbers to not use string format in match cycle metrics, from @laurameng

## [1.59.4] - 2022-04-14
### Changed
- Splitting 'updating dynamic clusters' log into separate entries, from @samincheva
- Convert match cycle log line to structured logging, from @laurameng

## [1.59.3] - 2022-03-31
### Added
- Add second init sidecar for checkpointing, from @scrosby
  - Add support for a second auxiliary init container for k8s that runs in the user's image in k8s, allowing it to introspect the platform and do any custom changes.
- Add Postgres support to Vagrant environment setup, from @nsinkov
### Changed
- Update sidecar dependencies and prepare to release sidecar 1.2.2, from @scrosby
### Fixed
- Corrected implementation of Kubernetes controller pod process log removal, from @laurameng

## [1.59.2] - 2022-03-09
### Added
- Add capability for configuring default Kubernetes pod labels on a per pool basis, from @laurameng
### Changed
- Reduced Cook logging to condense log volume, from @laurameng
  - Remove low-value Fenzo log as part of log diet efforts
  - Remove Kubernetes controller pod process logs on scans when Cook & Kubernetes agree on "running" state
  - Remove taskid scan log

## [1.59.1] - 2022-02-17
### Added
- Set USER env variable, in addition to COOK_JOB_USER, in Kubernetes by default, from @laurameng
### Changed
- Increase logging verbosity when submission fails with an exception, from @scrosby

## [1.58.12] - 2022-01-24
### Added
- Initial Liquibase support for Cook for postgres configuration, from @scrosby
### Changed
- Switch from java.jdbc to next.jdbc, from @scrosby
- Switch Cook to using c3p0 for database pooling, from @scrosby
- Switch OSS test runtime from Minimesos to GKE, from @scrosby

## [1.58.11] - 2022-01-14
### Fixed
- Fix pod label value validation regex, from @nsinkov

## [1.58.10] - 2022-01-07
### Added
- Reject jobs with invalid job constraints at submission time, from @nsinkov
- Reject jobs with invalid pob labels at submission time, from @nsinkov
- Support for setting annotation to use all group IDs in Kubernetes, from @dposada
- Ability for Cook to use a Postgres database, from @scrosby
### Fixed
- Add missing fields to compute cluster API validation, from @nsinkov

## [1.58.9] - 2021-12-09
### Added
- Metrics of gaps in Kubernetes watches, from @dposada
### Fixed
- Fix support for incremental default image configuration, from @nsinkov

## [1.58.8] - 2021-12-06
### Added
- Support using default image with a user-specified container, from @nsinkov

## [1.58.7] - 2021-12-01
### Added
- Allow incremental configurations for default job constraints, from @nsinkov

## [1.58.6] - 2021-11-19
### Added
- Field for command length, from @dposada
- Logging of job instance when rebalancer preemption transaction fails, from @dposada

## [1.58.5] - 2021-11-09
### Added
- Metrics for node and pod counts, from @nsinkov
- `:production?` to the config, from @dposada
### Changed
- Changed logging from ERROR to INFO when a deleted cluster's watch fails, from @dposada

## [1.58.4] - 2021-11-03
### Changed
- Changed logging with finalizer deletion, from @scrosby
- Collected metrics for waiting jobs under quota, from @nsinkov

## [1.58.3] - 2021-10-28
### Changed
- Cleaned up the 'no acceptable compute cluster' log, from @dposada

## [1.58.2] - 2021-10-22
### Changed
- Added support for transforming job constraints via configuration, from @dposada
### Fixed
- Filtering out unsound GPU nodes, from @dposada

## [1.58.1] - 2021-10-15
### Changed
- Support upserting > 1 incremental config in one transaction, from @nsinkov
### Fixed
- Fix compile error on ex-info call, from @scrosby
- Fix docker environment to work with clojure 1.10, from @scrosby

## [1.58.0] - 2021-10-11
### Changed
- Switch to JDK-11 and Clojure 1.10, from @scrosby

### Added
- Cook can add a finalizer to pods, from @scrosby
- Support for incremental config for checkpointing volume mounts, from @nsinkov
- Support for rotating logs every hour, from @scrosby

## [1.57.1] - 2021-09-29
### Changed
- Trimmed down pod event logging, from @dposada

## [1.57.0] - 2021-09-28
### Added
- Optional comments to incremental value configurations, from @nsinkov
### Changed
- Trimmed down pod metadata logging, from @dposada
### Fixed
- Reverting JDK11 upgrade (back to JDK8), from @nsinkov

## [1.56.2] - 2021-09-22
### Changed
- Reverted 1.10 clojure change, from @scrosby

## [1.56.1] - 2021-09-20
### Changed
- Add incremental image configuration support for aux containers, from @nsinkov
### Fixed
- Fix metric reporting that was broken in JDK-11, from @scrosby
- Make progress updates compatible with checkpointing, from @nsinkov

## [1.56.0] - 2021-09-14
### Changed
- Upgraded Cook to work with JDK11 and Clojure 1.10, from @scrosby 

## [1.55.2] - 2021-09-02
### Fixed
- Bug in default image selection logging, from @nsinkov

## [1.55.1] - 2021-09-01
### Added
- `exit-code` and `instance-exited?` to `pod-completed` passport events, from @dposada
### Changed
- Revamped pod-submission-related passport events, from @dposada
- Clarified not-looking-for-offers log, from @dposada

## [1.55.0] - 2021-08-27
### Added
- Incremental feature flags, from @nsinkov
- A flag for controlling which pools get telemetry-related environment variables, from @scrosby
- Support for defaulting environment variables by pool, from @scrosby
- Chunking to the `listPodForAllNamespaces` k8s API call, from @dposada
- Fast failing of job instances on 500 responses from k8s pod submissions, from @dposada

## [1.54.0] - 2021-08-04
### Added
- Support for shared memory on k8s, from @scrosby
- Resource requests to the `job-submitted` passport event, from @dposada
### Fixed
- Bug where `pod-launched` and `pod-completed` passport events sometimes have a `nil` pool, from @calebhar12

## [1.53.8] - 2021-07-29
### Fixed
- Adjust test_user_pool_rate_limit to make it more reliable, from @scrosby
- Fix Location header of redirects to include request parameters, from @scrosby
- Make /unscheduled endpoint redirect to leader, from @scrosby
- Avoid a lot of reflection costs in core Cook inner match and k8s loop, from @scrosby
- Use date in passport log file name, from @nsinkov
### Added
- Add pool-name, job-name, and user to Passport Logs, from @calebhar12
### Changed
- Update passport event types with cook-scheduler source and namespace, from @calebhar12
- Logs info instead of warn for node-watch timeouts, from @dposada

## [1.53.7] - 2021-07-15
### Fixed
- When adding a job to an existing job group, don't override the group, from @nsinkov
### Added
- Add pool source to job submission passport stamp, from @dposada
- Add instance uuid to job uuid cache, from @calebhar12

## [1.53.6] - 2021-07-06
### Added
- Save the submitted job's pool, from @dposada
- Ability to turn rebalancer on or off by pool, from @scrosby
### Changed
- Optimized a cache used by rebalancer, from @scrosby

## [1.53.5] - 2021-06-28
(internal-only release)

## [1.53.4] - 2021-06-23
(internal-only release)

## [1.53.3] - 2021-06-23
### Added
- Support for job-routing plugins, from @dposada

## [1.53.2] - 2021-06-01
### Added
- Features to compute clusters, from @dposada
### Changed
- Constrained checkpointing to supported pools, from @nsinkov

## [1.53.1] - 2021-05-13
### Fixed
- Fix the names for synthetic pod workload labels, from @dposada

## [1.53.0] - 2021-05-06
### Added
- Environment variables for telemetry, from @dposada
### Changed
- Improved performance for `VirtualMachineLeaseAdapter`, `TaskRequestAdapter`, and `update-host-reservation`, from @scrosby
### Fixed
- Hard delete pods that have been in the terminating state for too long, from @dposada
- Skip inactive pools when ranking, from @dposada

## [1.52.0] - 2021-04-28
### Added
- Instance field with how long the job queued before that instance, from @dposada
### Changed
- Made straggler kill a mea-culpa failure, from @nsinkov

## [1.51.14] - 2021-04-15
### Changed
- Added pod labels for application name and version, from @dposada
- Prefixed all application pod labels with the configured pod label prefix, from @dposada

## [1.51.13] - 2021-04-07
### Fixed
- Calculate the time-until-waiting metric correctly, from @scrosby

## [1.51.12] - 2021-04-06
### Changed
- Do fenzo unassigns in batches outside of the k8s state locks, from @scrosby
- Make the k8s lock vector a vector not a sequence, from @scrosby
- Split metrics for synthetic pods and regular pods in k8s, from @scrosby

## [1.51.11] - 2021-03-31
### Changed
- Split k8s lock shards by compute cluster, from @scrosby
- Do watch event processing in parallel at watch startup, from @scrosby
- Optimize novel host constraint by 10%, from @scrosby
- Prevent an inactive pool from having a scheduling loop, from @dposada
### Fixed
- Gracefully handle nodes with nil consumption maps, from @dposada

## [1.51.10] - 2021-03-30
### Changed
- Made prolonged `ContainersNotReady` pod condition result in failure, from @dposada
- Added logging of watch response status field, from @dposada

## [1.51.9] - 2021-03-26
### Changed
- Allowed synthetic pod anti-affinity to specify a namespace, from @dposada
- Improved logging when k8s watch response object is nil, from @dposada
### Fixed
- Gracefully ignore nodes with no pods during consumption calculation, from @dposada

## [1.51.8] - 2021-03-23
### Changed
- Allowed synthetic pods to have inter-pod anti-affinity, from @dposada

## [1.51.7] - 2021-03-11
### Added
- Make cook pods ignore a tenured node taint, from @scrosby
- Fix the memory request value sent to pod via environmental variable to exclude sidecar memory, from @nsinkov

## [1.51.6] - 2021-03-09
### Changed
- Allowed synthetic pods to have a non-default termination grace period, from @dposada

## [1.51.5] - 2021-03-08
### Changed
- A knob letting Cook clobber syhthetic pods with real jobs for k8s, from @scrosby
### Fixed
- Look for Cook memory labels on job labels, not pod labels, from @nsinkov

## [1.51.4] - 2021-03-04
### Changed
- Optimization to the match cycle, from @scrosby
- Add memory limit job label, from @nsinkov

## [1.51.3] - 2021-03-03
### Changed
- Optimized code for generating synthetic pods to do less work and autoscale less when we're matching more often, from @scrosby

## [1.51.2] - 2021-02-24
### Added
- Support for the default pool being a k8s pool, from @dposada
### Fixed
- Mark failure reason correctly for pod failure from preemption, from @dposada

## [1.51.1] - 2021-02-16
### Changed
- Support ignoring specific group ID's when computing supplemental group IDs, from @scrosby
- Log exceptions in `deep-merge-with`, from @dposada
### Fixed
- Take only the top X pending jobs when triggering k8s autoscaling, from @dposada

## [1.51.0] - 2021-02-01
### Added
- Support for longer pod names, from @scrosby
- Configurable validation of job resources by node type, from @dposada
- `/usage` for all users, from @dposada
### Changed
- Make `job->acceptable-compute-clusters` configurable, from @dposada

## [1.50.1] - 2021-01-14
### Added
- Ability to not set memory limits, from @kathryn-zhou
- Authenticator refresh logic needed for non-GKE k8s, from @scrosby
- Checkpoint locality constraint, from @dposada
### Changed
- Logging the largest job and offer by resource, from @dposada

## [1.50.0] - 2020-12-21
### Added
- Adds location to compute cluster, from @dposada
- Schedules and matches jobs with disk, from @kathryn-zhou
### Changed
- Makes k8s API client read timeout configurable, from @dposada

## [1.49.2] - 2020-12-04
### Added
- Add resource request and limit to init-container in pod, from @scrosby
### Changed
- Refactor authentication initialization, from @scrosby
- Migrate to GitHub Actions from Travis CI, from @kevo1ution
### Fixed
- Allow users to use int values for disk request and disk limit, from @kathryn-zhou

## [1.49.1] - 2020-11-19
### Added
- Add support for ignoring a taint prefix, from @scrosby
### Changed
- Increase limit for launch-task-num-threads, from @scrosby
- Make progress an absolute path in k8s, from @scrosby
### Fixed
- Do not schedule nodes with unschedulable node-spec, from @scrosby
- Improve error handling when calculating effective image, from @nsinkov

## [1.49.0] - 2020-11-11
### Added
- Per-user queue length limits, from @dposada
- API for Disk Limits, from @kathryn-zhou
- Metadata pod env vars, from @nsinkov
- Support for modifying pod image when checkpointing, from @nsinkov
### Changed
- Increases default and max :controller-lock-num-shards, from @dposada
- Make the kill-lock be a ReentrantReadWriteLock and add metrics, from @scrosby
- Make pool taint / label and context configurable, from @scrosby
### Fixed
- Gracefully handles unknown job resource type, from @dposada
- Fix memory leak in k8s state for deleted pods, from @scrosby

## [1.48.4] - 2020-10-28
### Changed
- Reduced excessive logging for checkpointing and launching tasks, from @dposada

## [1.48.3] - 2020-10-23
### Changed
- Added supplemental groups to the pod security context, from @dposada

## [1.48.2] - 2020-10-19
### Changed
- Reduced excessive logging for k8s dynamic clusters and writing tasks, from @dposada

## [1.48.1] - 2020-10-15
### Changed
- Cache sizes to be configurable, from @scrosby
- Tracking of how rate limiting is affecting the queue, from @scrosby

## [1.48.0] - 2020-10-08
### Added
- Per-user per-pool job launch rate limiting, from @scrosby
- Configurable checkpointing kill switch, from @nsinkov
### Changed
- Dynamic compute cluster log from `ERROR` to `WARN`, from @scrosby
### Fixed
- Allowing for different rate limit for auth-bypass requests, from @dposada

## [1.47.1] - 2020-10-01
### Changed
- Added warning log when jobs go unmatched for too long, from @dposada
- Added the ability to flush a rate limit from the cache, from @scrosby

## [1.47.0] - 2020-09-25
### Added
- Add rate limits per compute cluster, from @scrosby
### Changed
- Cached job-constant fields in defrecords for gpu-host-constraint, from @kathryn-zhou
- Cache job-constant fields in defrecords for user-defined-constraint, from @kathryn-zhou

## [1.46.0] - 2020-09-21
### Added
- Workload fields to job application, from @dposada

## [1.45.2] - 2020-09-15
### Changed
- Reduced excessive logging for k8s, from @dposada
- Added logging of offer and job resource percentiles, from @dposada
### Fixed
- Missing compute cluster check, from @nsinkov
- Deleting unschedulable synthetic pods, from @dposada

## [1.45.1] - 2020-09-08
### Added
- Dynamic cluster configuration support, from @nsinkov
### Changed
- Improved logging for launching tasks, stop launching synthetic pods, and matching offers, from @dposada

## [1.45.0] - 2020-08-25
### Added
- Support for EQUALS job constraints in k8s, from @dposada
- `HOST_IP` environment variable for k8s, from @dposada
### Changed
- De-lazied the list of constraints to avoid locking in Fenzo, from @scrosby
- Made `job->previous-hosts-to-avoid` use `set` instead of `mapv -> distinct` to reduce lock contention, from @sradack
### Fixed
- Order of per-user and pool-global quota application, from @scrosby

## [1.44.0] - 2020-08-17
### Added
- Support for prefixed job labels to become k8s pod labels, from @dposada
- Attribution labels to k8s synthetic pods, from @dposada
- `agent_id` as a preferred alternative to `slave_id` on job instances, from @dposada
- `/shutdown-leader` admin-only API endpoint, from @dposada
### Changed
- Improved matching log, from @dposada
- Improved per-user launch-rate-limit log, from @dposada
- Added log at start and end of job ranking, from @dposada
- Made "killing cancelled task" log INFO-level, from @dposada
### Fixed
- Handling of preemption on k8s pod initialization, from @nsinkov

## [1.43.2] - 2020-07-30
### Fixed
- Using 1024*1024 (mebibytes) as the k8s memory multiplier, from @dposada
- Avoiding NPE due to missing resources when totaling resources for metrics, from @dposada 

## [1.43.1] - 2020-07-24
### Added
- Per-pool global quotas, from @scrosby
### Fixed
- Accounting for GPU tasks assigned to nodes in the current matching cycle, from @kathryn-zhou
- Force processing when state scanning in k8s, from @dposada

## [1.43.0] - 2020-07-23
### Added
- GPU job support in k8s, from @kathryn-zhou

## [1.42.6] - 2020-07-17
### Added
- Rapid pool skipping in k8s, from @nsinkov
- Enhanced offer generation and updated GPU constraints for k8s, from @kathryn-zhou
### Fixed
- Fixed some errors that caused NPE and ERROR logs, from @scrosby
- Replaced chime logic with less aggressive chime logic, from @nsinkov

## [1.42.5] - 2020-07-08
### Added
- Port mapping support for k8s, from @dposada
- Total number of pods and nodes quota for k8s, from @scrosby
### Fixed
- Fixed several O(#pods * #nodes) bugs in k8s code, from @scrosby
- Limit autoscaling to quota of what's allowed to run, from @dposada
- Improved pool scheduling by fixing chime logic, from @nsinkov

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
- Support to blocklist nodes in k8s that have certain labels, from @scrosby
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
- Renamed to users allowed, from @dposada
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
- Added support for user allowlisting, from @dposada

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
