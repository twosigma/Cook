/*
 * Copyright (c) Two Sigma Open Source, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twosigma.cook.jobclient;

import java.util.*;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;
import com.twosigma.cook.jobclient.constraint.Constraints;
import com.twosigma.cook.jobclient.constraint.Constraint;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.base.Preconditions;

/**
 * An immutable job implementation.
 * <p>
 * A job could be consist of multiple {@link Instance}s. An instance of a job is a realization of
 * the job. The transition graph of a job status is INITIALIZED -> WAITING -> RUNNING -> COMPLETED
 * where<br>
 * -- INITIALIZED specifies a job status that the job is just created or initialized.<br>
 * -- WAITING specifies a job status that the job has been submitted to scheduler but it is waiting
 * for resources.<br>
 * -- RUNNING specifies a job status that the job has at least one instance running (possibly
 * previous instances have failed).<br>
 * -- COMPLETED specifies a job status that the job (1) has tried out maximum number of instances
 * and all tried instances have failed (2) or has one successful instance before trying out maximum
 * number of instances .<br>
 * Thus, a job is successful if and only if it has one successful instance.
 * <p>
 * Note that a job in the Cook scheduler only has three statuses: WAITING, RUNNING, COMPLETED. The
 * INITIALIZED status is for {@link JobListener} to receive {@link Status} update.
 * <p>
 * Created: March 14, 2015
 *
 * @author wzhao
 */
final public class Job {
    /**
     * A job in cook scheduler only has three statuses: WAITING, RUNNING, COMPLETED. The INITIALIZED status is for
     * {@link JobListener} to receive {@link Job.Status} update.
     */
    public static enum Status {
        INITIALIZED("INITIALIZED"), WAITING("WAITING"), RUNNING("RUNNING"), COMPLETED("COMPLETED");

        private Status(String name) {
        }

        /**
         * @param name specifies a string representation of status.
         * @return a constant for the specified name.
         */
        public static Status fromString(String name) {
            return Enum.valueOf(Status.class, name.trim().toUpperCase());
        }
    }

    /**
     * Job builder
     */
    public static class Builder {
        private UUID _uuid;
        private String _name;
        private String _command;
        private Executor _executor;
        private Double _memory;
        private Double _cpus;
        private Integer _retries;
        private Long _maxRuntime;
        private Long _expectedRuntime;
        private Status _status;
        private Integer _priority;
        private Boolean _isMeaCulpaRetriesDisabled;
        private List<Instance> _instances = Collections.synchronizedList(new ArrayList<Instance>());
        private List<FetchableURI> _uris = new ArrayList<>();
        private Map<String, String> _env = new HashMap<>();
        private JSONObject _container;
        private Map<String, String> _labels = new HashMap<>();
        private List<UUID> _groups = new ArrayList<>();
        // Use LinkedHashSet to ensure the insertion order will be kept.
        private Set<Constraint> _constraints = new LinkedHashSet<>();
        private Application _application;
        private String _progressOutputFile;
        private String _progressRegexString;
        private String _user;

        /**
         * Prior to {@code build()}, command, memory and cpus for a job must be provided.<br>
         * - If the job UUID is not provided, the job will be assigned a random UUID.<br>
         * - If the job status is not provided, the job status will set to INITIALIZED.<br>
         * - If the job retries is not provided, the job retries will set to 5.
         *
         * @return a instance of {@link Job}.
         */
        public Job build() {
            Preconditions.checkNotNull(_command, "command must be set!");
            Preconditions.checkNotNull(_memory, "memory must be set!");
            Preconditions.checkNotNull(_cpus, "cpus must be set!");

            // Set the default values.
            if (_uuid == null) {
                _uuid = UUID.randomUUID();
            }
            if (_retries == null) {
                _retries = 5;
            }
            if (_maxRuntime == null) {
                _maxRuntime = Long.MAX_VALUE;
            }
            if (_priority == null) {
                _priority = 50;
            }
            if (_status == null) {
                _status = Status.INITIALIZED;
            }
            if (_name == null) {
                _name = "cookjob";
            }
            if (_isMeaCulpaRetriesDisabled == null) {
                _isMeaCulpaRetriesDisabled = false;
            }
            return new Job(_uuid, _name, _command, _executor, _memory, _cpus, _retries, _maxRuntime, _expectedRuntime, _status,
                    _priority, _isMeaCulpaRetriesDisabled, _instances, _env, _uris, _container, _labels, _constraints,
                    _groups, _application, _progressOutputFile, _progressRegexString, _user);
        }

        /**
         * Set command, memory, cpus, env vars, uris, and retries from a job.
         *
         * @param job {@link Job} specifies a job.
         * @return this builder.
         */
        public Builder of(Job job) {
            setCommand(job.getCommand());
            setExecutor(job.getExecutor());
            setMemory(job.getMemory());
            setCpus(job.getCpus());
            setRetries(job.getRetries());
            setMaxRuntime(job.getMaxRuntime());
            setEnv(job.getEnv());
            setUris(job.getUris());
            setContainer(job.getContainer());
            setLabels(job.getLabels());
            if (job.isMeaCulpaRetriesDisabled()) {
                disableMeaCulpaRetries();
            } else {
                enableMeaCulpaRetries();
            }
            return this;
        }

        /**
         * Add a URI to the job
         *
         * @param uri The uri to fetch for the job
         * @return this builder
         */
        public Builder addUri(FetchableURI uri) {
            _uris.add(uri);
            return this;
        }

        /**
         * Adds the URIs to the job.
         * <p>
         * Note that this keeps previously added URIs.
         *
         * @param uris A list of URIs to add to this job
         * @return this builder
         */
        public Builder addUris(Collection<FetchableURI> uris) {
            _uris.addAll(uris);
            return this;
        }

        /**
         * Adds the job to a group by UUID
         *
         * @param guuid A group to which the job belongs by UUID
         * @return this builder
         */
        private Builder _setGroupByUUID(UUID guuid) {
            _groups.clear();
            _groups.add(guuid);
            return this;
        }

        /**
         * Adds the job to a group
         *
         * @param group A group to which the job belongs
         * @return this builder
         */
        public Builder setGroup(Group group) {
            _setGroupByUUID(group.getUUID());
            return this;
        }

        /**
         * Adds a constraint.
         *
         * @param constraint The constraint to add
         * @return this builder.
         */
        public Builder addConstraint(Constraint constraint) {
            _constraints.add(constraint);
            return this;
        }

        /**
         * Adds a collection of constraints.
         *
         * @param constraints The constraints to add
         * @return this builder.
         */
        public Builder addConstraint(Collection constraints) {
            _constraints.addAll(constraints);
            return this;
        }

        /**
         * Resets the URIs of the job to the given collection.
         *
         * @param uris A collection of URIs to use for this job
         * @return this builder
         */
        public Builder setUris(Collection<FetchableURI> uris) {
            _uris.clear();
            _uris.addAll(uris);
            return this;
        }

        /**
         * Add an env var to the job
         *
         * @param name  specifies the name of the env var
         * @param value specifies the value of the env var
         * @return this builder
         */
        public Builder addEnv(String name, String value) {
            _env.put(name, value);
            return this;
        }

        /**
         * Adds a collection of env vars to the job.
         * <p>
         * This adds the enviroment to the job; it won't remove variables that were previously set.
         *
         * @param environment specifies the environment to add to this job.
         * @return this builder
         */
        public Builder addEnv(Map<String, String> environment) {
            _env.putAll(environment);
            return this;
        }

        /**
         * Adds a collection of env vars to the job.
         * <p>
         * This resets the enviroment to the job; it will remove variables that were previously set.
         *
         * @param environment specifies the environment to set for this job.
         * @return this builder
         */
        public Builder setEnv(Map<String, String> environment) {
            _env = ImmutableMap.copyOf(environment);
            return this;
        }

        /**
         * Add an label to the job
         *
         * @param key   specifies the key of the label
         * @param value specifies the value of the label
         * @return this builder
         */
        public Builder addLabel(String key, String value) {
            _labels.put(key, value);
            return this;
        }

        /**
         * Adds a collection of labels to the job.
         * <p>
         * This adds the labels to the job; it won't remove labels that were previously set.
         *
         * @param labels specifies the labelironment to add to this job.
         * @return this builder
         */
        public Builder addLabels(Map<String, String> labels) {
            _labels.putAll(labels);
            return this;
        }

        /**
         * Adds a collection of label to the job.
         * <p>
         * This resets the labels of the job; it will remove labels that were previously set.
         *
         * @param labels specifies the labels to set for this job.
         * @return this builder
         */
        public Builder setLabels(Map<String, String> labels) {
            _labels = ImmutableMap.copyOf(labels);
            return this;
        }

        /**
         * Set the UUID of the job expected to build.
         *
         * @param uuid {@link UUID} specifies the unique identifier for a job.
         * @return this builder.
         */
        public Builder setUUID(UUID uuid) {
            _uuid = uuid;
            return this;
        }

        /**
         * Set the command of the job expected to build.
         *
         * @param command {@link String} specifies command for a job.
         * @return this builder.
         */
        public Builder setCommand(String command) {
            _command = command;
            return this;
        }

        /**
         * Set the executor of the job expected to build.
         *
         * @param executor {@link String} specifies executor for a job.
         * @return this builder.
         */
        public Builder setExecutor(String executor) {
            return setExecutor(Executor.fromString(executor));
        }

        /**
         * Set the executor of the job expected to build.
         *
         * @param executor {@link Executor} specifies executor for a job.
         * @return this builder.
         */
        public Builder setExecutor(Executor executor) {
            _executor = executor;
            return this;
        }

        /**
         * Set the cpus of the job expected to build.
         *
         * @param cpus {@link Double} specifies cpus for a job.
         * @return this builder.
         */
        public Builder setCpus(Double cpus) {
            _cpus = cpus;
            return this;
        }

        /**
         * Set the memory of the job expected to build.
         *
         * @param memory {@link Double} specifies memory for a job.
         * @return this builder.
         */
        public Builder setMemory(Double memory) {
            _memory = memory;
            return this;
        }

        /**
         * Set the container information of the job expected ot build.
         *
         * @param container {@link JSONObject} specifies container information for the job
         * @return this builder.
         */
        public Builder setContainer(JSONObject container) {
            _container = container;
            return this;
        }

        /**
         * Set the number of retires of the job expected to build.
         *
         * @param retries {@link Integer} specifies the number of retires for a job.
         * @return this builder.
         */
        public Builder setRetries(Integer retries) {
            Preconditions.checkArgument(retries > 0, "The number of retries must be > 0.");
            _retries = retries;
            return this;
        }

        /**
         * Disable the "mea-culpa" retries.
         *
         * @return this builder.
         * @see <a href ="https://github.com/twosigma/Cook/blob/master/scheduler/docs/faq.md#how-can-i-configure-my-job-to-run-exactly-once">
         * how-can-i-configure-my-job-to-run-exactly-once
         * </a>
         */
        public Builder disableMeaCulpaRetries() {
            _isMeaCulpaRetriesDisabled = true;
            return this;
        }

        /**
         * Enable the "mea-culpa" retries.
         *
         * @return this builder.
         * @see <a href ="https://github.com/twosigma/Cook/blob/master/scheduler/docs/faq.md#how-can-i-configure-my-job-to-run-exactly-once">
         * how-can-i-configure-my-job-to-run-exactly-once
         * </a>
         */
        public Builder enableMeaCulpaRetries() {
            _isMeaCulpaRetriesDisabled = false;
            return this;
        }

        /**
         * Set the maximum runtime in milliseconds of the job expected to build.
         *
         * @param runtime {@link Long} specifies the maximun runtime in milliseconds for a job.
         * @return this builder.
         */
        public Builder setMaxRuntime(Long runtime) {
            Preconditions.checkArgument(runtime > 0, "The max runtime in milliseconds must be > 0.");
            _maxRuntime = runtime;
            return this;
        }

        /**
         * Set the expected runtime in milliseconds of the job expected to build.
         *
         * @param runtime {@link Long} specifies the expected runtime in milliseconds for a job.
         * @return this builder.
         */
        public Builder setExpectedRuntime(long runtime) {
            Preconditions.checkArgument(runtime > 0, "The expected runtime in milliseconds must be > 0.");
            _expectedRuntime = runtime;
            return this;
        }

        /**
         * Set the status of the job expected to build.
         *
         * @param status {@link Status} specifies the status for a job.
         * @return this builder.
         */
        public Builder setStatus(Status status) {
            _status = status;
            return this;
        }

        /**
         * Set the name of the job expected to build.
         *
         * @param name {@link String} specifies the name for a job.
         * @return this builder.
         */
        public Builder setName(String name) {
            final Pattern pattern = Pattern.compile("[\\.a-zA-Z0-9_-]{0,128}");
            Preconditions.checkArgument
                    (pattern.matcher(name).matches(),
                            "Name can only contain '.', '_', '-' or any work characters has length at most 128");
            _name = name;
            return this;
        }

        /**
         * Set the priority of the job expected to build.
         *
         * @param priority {@link Integer} specifies the priority of the job
         * @return this builder.
         */
        public Builder setPriority(Integer priority) {
            _priority = priority;
            return this;
        }

        /**
         * Add an instance to the job expected to build.
         *
         * @param instance {@link Instance} specifies an instance for a job.
         * @return this builder.
         */
        public Builder addInstance(Instance instance) {
            _instances.add(instance);
            return this;
        }

        /**
         * Add a list of instances to the job expected to build.
         *
         * @param instances specifies a list of instances for a job.
         * @return this builder
         */
        public Builder addInstances(List<Instance> instances) {
            _instances.addAll(instances);
            return this;
        }

        /**
         * Sets the application of the job expected to build.
         *
         * @param application {@link Application} specifies the application of the job.
         * @return this builder.
         */
        public Builder setApplication(Application application) {
            _application = application;
            return this;
        }

        /**
         * Set the progress output file of the job expected to build.
         * It can be an absolute path or a path relative to the sandbox directory.
         *
         * @param progressOutputFile {@link String} specifies the progress output file for the job.
         * @return this builder.
         */
        public Builder setProgressOutputFile(String progressOutputFile) {
            _progressOutputFile = progressOutputFile;
            return this;
        }

        /**
         * Set the progress regex string of the job expected to build.
         * The progress regex to match against, it must return one or two capture groups.
         * The first capture group represents the progress percentage.
         * The second capture group, if present, represents the progress message.
         *
         * @param progressRegexString {@link String} specifies the progress regex string for the job.
         * @return this builder.
         */
        public Builder setProgressRegexString(String progressRegexString) {
            _progressRegexString = progressRegexString;
            return this;
        }

        /**
         * Set the user of the job expected to build.
         *
         * @param user {@link String} specifies the user for a job.
         * @return this builder.
         */
        public Builder setUser(String user) {
            _user = user;
            return this;
        }
    }

    final private UUID _uuid;
    final private String _name;
    final private String _command;
    final private Executor _executor;
    final private Double _memory;
    final private Double _cpus;
    final private Integer _retries;
    final private Long _maxRuntime;
    final private Long _expectedRuntime;
    final private Integer _priority;
    final private Status _status;
    final private Boolean _isMeaCulpaRetriesDisabled;
    final private List<Instance> _instances;
    final private Map<String, String> _env;
    final private List<FetchableURI> _uris;
    final private JSONObject _container;
    final private Map<String, String> _labels;
    final private Set<Constraint> _constraints;
    // This is a list although for now each job is only allowed to belong to one group (see setGroup and getGroup). In
    // the future, jobs will be allowed to belong to multiple groups.
    final private List<UUID> _groups;
    final private Application _application;
    final private String _progressOutputFile;
    final private String _progressRegexString;
    final private String _user;

    private Job(UUID uuid, String name, String command, Executor executor, Double memory, Double cpus, Integer retries,
                Long maxRuntime, Long expectedRuntime, Status status, Integer priority, Boolean isMeaCulpaRetriesDisabled,
                List<Instance> instances, Map<String, String> env, List<FetchableURI> uris, JSONObject container,
                Map<String, String> labels, Set<Constraint> constraints, List<UUID> groups, Application application,
                String progressOutputFile, String progressRegexString, String user) {
        _uuid = uuid;
        _name = name;
        _command = command;
        _executor = executor;
        _memory = memory;
        _cpus = cpus;
        _retries = retries;
        _maxRuntime = maxRuntime;
        _expectedRuntime = expectedRuntime;
        _status = status;
        _priority = priority;
        _isMeaCulpaRetriesDisabled = isMeaCulpaRetriesDisabled;
        _instances = ImmutableList.copyOf(instances);
        _env = ImmutableMap.copyOf(env);
        _uris = ImmutableList.copyOf(uris);
        _application = application;
        _progressOutputFile = progressOutputFile;
        _progressRegexString = progressRegexString;
        _user = user;
        // This take the string representation of the JSON object and then parses it again which is inefficient but
        // that is most convenient way to deep copy a JSONObject and make this Job instance immutable.
        if (container != null) {
            try {
                _container = new JSONObject(container.toString());
            } catch (JSONException e) {
                throw new RuntimeException("Failed to parse the container string", e);
            }
        } else {
            _container = null;
        }
        _labels = ImmutableMap.copyOf(labels);
        _constraints = ImmutableSet.copyOf(constraints);
        _groups = groups;
    }

    /**
     * @return the job UUID.
     */
    public UUID getUUID() {
        return _uuid;
    }

    /**
     * @return the job command.
     */
    public String getCommand() {
        return _command;
    }

    /**
     * @return the job executor.
     */
    public Executor getExecutor() {
        return _executor;
    }

    /**
     * @return the job memory.
     */
    public Double getMemory() {
        return _memory;
    }

    /**
     * @return the job cpus.
     */
    public Double getCpus() {
        return _cpus;
    }

    /**
     * @return the job retries.
     */
    public Integer getRetries() {
        return _retries;
    }

    /**
     * @return the max runtime in milliseconds for this job.
     */
    public Long getMaxRuntime() {
        return _maxRuntime;
    }

    /**
     * @return the expected runtime in milliseconds for this job.
     */
    public Long getExpectedRuntime() {
        return _expectedRuntime;
    }

    /**
     * @return the job's environment
     */
    public Map<String, String> getEnv() {
        return _env;
    }

    /**
     * @return the job's labels
     */
    public Map<String, String> getLabels() {
        return _labels;
    }


    public Set<Constraint> getConstraints() {
        return _constraints;
    }

    /**
     * @return the job's group, or null if the job does not belong to a group
     */
    public UUID getGroup() {
        if (_groups.size() > 0) {
            return _groups.get(0);
        }
        return null;
    }

    /**
     * @return the job's container
     */
    public JSONObject getContainer() {
        return _container;
    }

    /**
     * @return the job's uris
     */
    public List<FetchableURI> getUris() {
        return _uris;
    }

    /**
     * @return the job user.
     */
    public String getUser() {
        return _user;
    }

    /**
     * @return the job status.
     */
    public Status getStatus() {
        return _status;
    }

    /**
     * @return the job name.
     */
    public String getName() {
        return _name;
    }

    /**
     * @return the job priority.
     */
    public Integer getPriority() {
        return _priority;
    }

    /**
     * @return whether "mea-culpa" retries is disabled.
     * @see <a href ="https://github.com/twosigma/Cook/blob/master/scheduler/docs/faq.md#how-can-i-configure-my-job-to-run-exactly-once">
     * how-can-i-configure-my-job-to-run-exactly-once
     * </a>
     */
    public Boolean isMeaCulpaRetriesDisabled() {
        return _isMeaCulpaRetriesDisabled;
    }

    /**
     * @return the job instances.
     */
    public List<Instance> getInstances() {
        return _instances;
    }

    /**
     * @return the job application.
     */
    public Application getApplication() {
        return _application;
    }

    /**
     * The progress output file configured for the job.
     * It is either an absolute path or a path relative to the sandbox directory.
     *
     * @return the progress output file configured for the job. It returns null if not configured.
     */
    public String getProgressOutputFile() {
        return _progressOutputFile;
    }

    /**
     * The progress regex to match against, it must return one or two capture groups.
     * The first capture group represents the progress percentage.
     * The second capture group, if present, represents the progress message.
     *
     * @return the progress regex string configured for the job. It returns null if not configured.
     */
    public String getProgressRegexString() {
        return _progressRegexString;
    }

    /**
     * @return the job instance with the running state or {@code null} if can't find one.
     */
    public Instance getRunningInstance() {
        for (Instance instance : _instances) {
            if (Instance.Status.RUNNING.equals(instance.getStatus())) {
                return instance;
            }
        }
        return null;
    }

    /**
     * A job is successful if and only if the job is completed and one of its instances is successful.
     *
     * @return
     */
    public Boolean isSuccess() {
        if (getStatus() == Job.Status.COMPLETED) {
            for (Instance instance : getInstances()) {
                if (instance.getStatus() == Instance.Status.SUCCESS)
                    return true;
            }
        }
        return false;
    }

    /**
     * Convert a job to a JSON object, e.g.
     * <p>
     * <pre>
     * <code>
     * {
     *     "max_retries" : 3,
     *     "mem" : 1000,
     *     "uuid" : "26719da8-394f-44f9-9e6d-8a17500f5109",
     *     "cpus" : 1.5,
     *     "priority" : 60,
     *     "command" : "echo hello world",
     *     "status" : "waiting"
     * }
     * </code>
     * </pre>
     *
     * @param job specifies a job.
     * @return a JSON object which represents a job.
     * @throws JSONException
     */
    public static JSONObject jsonizeJob(Job job)
            throws JSONException {
        final JSONObject env = new JSONObject(job.getEnv());
        final JSONObject labels = new JSONObject(job.getLabels());
        final JSONObject container = job.getContainer();
        final JSONObject object = new JSONObject();
        object.put("uuid", job.getUUID().toString());
        object.put("name", job.getName());
        object.put("command", job.getCommand());
        if (job.getExecutor() != null) {
            object.put("executor", job.getExecutor().displayName());
        }
        object.put("mem", job.getMemory());
        object.put("cpus", job.getCpus());
        object.put("priority", job.getPriority());
        object.put("max_retries", job.getRetries());
        object.put("disable_mea_culpa_retries", job.isMeaCulpaRetriesDisabled());
        object.put("max_runtime", job.getMaxRuntime());
        object.put("env", env);
        object.put("labels", labels);
        // For now, only use one group
        UUID group = job.getGroup();
        if (group != null) {
            object.put("group", job.getGroup().toString());
        }
        for (Constraint constraint : job.getConstraints()) {
            object.append("constraints", constraint.toJson());
        }
        if (container != null) {
            object.put("container", container);
        }
        for (FetchableURI uri : job.getUris()) {
            object.append("uris", FetchableURI.jsonizeUri(uri));
        }
        if (job._application != null) {
            object.put("application", Application.jsonizeApplication(job._application));
        }
        if (job._progressOutputFile != null) {
            object.put("progress_output_file", job._progressOutputFile);
        }
        if (job._progressRegexString != null) {
            object.put("progress_regex_string", job._progressRegexString);
        }
        if (job._expectedRuntime != null) {
            object.put("expected_runtime", job._expectedRuntime);
        }
        return object;
    }

    /**
     * Convert a list of job to a JSON object, e.g.
     * <p>
     * <pre>
     * <code>
     * {
     *    "jobs" : [
     *       {
     *          "max_retries" : 3,
     *          "mem" : 1000,
     *          "uuid" : "26719da8-394f-44f9-9e6d-8a17500f5109",
     *          "priority" : 60,
     *          "cpus" : 1.5,
     *          "command" : "echo hello world",
     *          "status" : "waiting"
     *       }
     *    ]
     * }
     * </code>
     * </pre>
     * <p>
     * The converted JSON object could be used for job submission via Cook client.
     *
     * @param jobs specifies a collection of jobs.
     * @return a JSON object which represent a list jobs.
     * @throws JSONException
     */
    public static JSONObject jsonizeJob(Collection<Job> jobs)
            throws JSONException {
        final ArrayList<JSONObject> objects = new ArrayList<JSONObject>();
        for (final Job job : jobs) {
            objects.add(jsonizeJob(job));
        }
        final JSONObject object = new JSONObject();
        object.put("jobs", objects);
        return object;
    }

    /**
     * Parse a JSON string representing a list of jobs, e.g.
     * <p>
     * <pre>
     * <code>
     * [
     *    {
     *       "instances" : [
     *          {
     *             "slave_id" : "20150311-033720-1963923116-5050-4084-32",
     *             "end_time" : 1426632251828,
     *             "status" : "success",
     *             "start_time" : 1426632249597,
     *             "hostname" : "server1.example.com",
     *             "executor_id" : "f52fbacf-52a1-44a2-bda1-cbfa477cc163",
     *             "task_id" : "f52fbacf-52a1-44a2-bda1-cbfa477cc163",
     *             "preempted": false
     *          }
     *       ],
     *       "status" : "completed",
     *       "mem" : 1000,
     *       "uuid" : "26719da8-394f-44f9-9e6d-8a17500f5109",
     *       "cpus" : 1.5,
     *       "command" : "echo hello world",
     *       "framework_id" : null
     *    }
     * ]
     * </code>
     * </pre>
     *
     * @param listOfJobs {@link String} specifies a list of jobs.
     * @param decorator  specifies an instance decorator expected to decorate instances parsed from JSON string.
     *                   If it is null, it will do nothing with it.
     * @return a list of {@link Job}s.
     * @throws JSONException
     */
    public static List<Job> parseFromJSON(String listOfJobs, InstanceDecorator decorator)
            throws JSONException {
        JSONArray jsonArray = new JSONArray(listOfJobs);
        List<Job> jobs = new ArrayList<Job>(jsonArray.length());
        for (int i = 0; i < jsonArray.length(); ++i) {
            JSONObject json = jsonArray.getJSONObject(i);
            Builder jobBuilder = new Builder();
            jobBuilder.setUUID(UUID.fromString(json.getString("uuid")));
            jobBuilder.setMemory(json.getDouble("mem"));
            jobBuilder.setCpus(json.getDouble("cpus"));
            jobBuilder.setCommand(json.getString("command"));
            if (json.has("executor")) {
                jobBuilder.setExecutor(json.getString("executor"));
            }
            jobBuilder.setPriority(json.getInt("priority"));
            jobBuilder.setStatus(Status.fromString(json.getString("status")));
            if (json.has("disable_mea_culpa_retries") && json.getBoolean("disable_mea_culpa_retries")) {
                jobBuilder.disableMeaCulpaRetries();
            } else {
                jobBuilder.enableMeaCulpaRetries();
            }
            if (json.has("name")) {
                jobBuilder.setName(json.getString("name"));
            }
            if (json.has("user")) {
                jobBuilder.setUser(json.getString("user"));
            }
            jobBuilder.setRetries(json.getInt("max_retries"));
            jobBuilder.setMaxRuntime(json.getLong("max_runtime"));
            if (json.has("container")) {
                jobBuilder.setContainer(json.getJSONObject("container"));
            }
            if (json.has("env")) {
                JSONObject envJson = json.getJSONObject("env");
                Map<String, String> envMap = new HashMap<>();
                if (envJson.length() > 0) {
                    for (String varName : JSONObject.getNames(envJson)) {
                        envMap.put(varName, envJson.getString(varName));
                    }
                }
                jobBuilder.setEnv(envMap);
            }
            if (json.has("labels")) {
                JSONObject labelsJson = json.getJSONObject("labels");
                Map<String, String> labelsMap = new HashMap<>();
                if (labelsJson.length() > 0) {
                    for (String varName : JSONObject.getNames(labelsJson)) {
                        labelsMap.put(varName, labelsJson.getString(varName));
                    }
                }
                jobBuilder.setLabels(labelsMap);
            }
            JSONArray urisJson = json.optJSONArray("uris");
            if (urisJson != null) {
                for (int j = 0; j < urisJson.length(); j++) {
                    jobBuilder.addUri(FetchableURI.parseFromJSON(urisJson.getJSONObject(j)));
                }
            }
            if (json.has("constraints")) {
                JSONArray constraintsJson = json.getJSONArray("constraints");
                for (int j = 0; j < constraintsJson.length(); j++) {
                    jobBuilder.addConstraint(Constraints.parseFrom(constraintsJson.getJSONArray(j)));
                }
            }
            JSONArray groupsJson = json.optJSONArray("groups");
            if (groupsJson != null) {
                for (int j = 0; j < groupsJson.length(); j++) {
                    jobBuilder._setGroupByUUID(UUID.fromString(groupsJson.getString(j)));
                }
            }
            jobBuilder.addInstances(Instance.parseFromJSON(json.getJSONArray("instances"), decorator));
            if (json.has("application")) {
                JSONObject applicationJson = json.getJSONObject("application");
                jobBuilder.setApplication(Application.parseFromJSON(applicationJson));
            }
            if (json.has("expected_runtime")) {
                jobBuilder.setExpectedRuntime(json.getLong("expected_runtime"));
            }
            if (json.has("progress_output_file")) {
                jobBuilder.setProgressOutputFile(json.getString("progress_output_file"));
            }
            if (json.has("progress_regex_string")) {
                jobBuilder.setProgressRegexString(json.getString("progress_regex_string"));
            }
            jobs.add(jobBuilder.build());
        }
        return jobs;
    }

    /**
     * Similar to {@code List<Job> parseFromJSON(String listOfJobs, InstanceDecorator decorator) with {@code decorator}
     * being {@code null}.
     *
     * @param listOfJobs {@link String} specifies a list of jobs.
     * @return a list of {@link Job}s.
     * @throws JSONException
     */
    public static List<Job> parseFromJSON(String listOfJobs) throws JSONException {
        return parseFromJSON(listOfJobs, null);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder(512);
        stringBuilder
                .append("Job [_uuid=" + _uuid + ", _name=" + _name + ", _command=" + _command + ", _executor=" + _executor
                    + ", _memory=" + _memory + ", _cpus=" + _cpus + ", _retries=" + _retries
                    + ", _maxRuntime=" + _maxRuntime + ", _status=" + _status + ", _priority=" + _priority
                    + ", _progressOutputFile=" + _progressOutputFile + ", _progressRegexString=" + _progressRegexString
                    + ", _isMeaCulpaRetriesDisabled=" + _isMeaCulpaRetriesDisabled + ", _user=" + _user + "]");
        stringBuilder.append('\n');
        for (Instance instance : getInstances()) {
            stringBuilder.append(instance.toString()).append('\n');
        }
        return stringBuilder.toString();
    }

    /**
     * A job is equivalent to another if and only if<br>
     * - their task UUIDs are the same<br>
     * - their task statuses are the same<br>
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_status == null) ? 0 : _status.hashCode());
        result = prime * result + ((_uuid == null) ? 0 : _uuid.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!getClass().equals(obj.getClass()))
            return false;
        Job other = (Job) obj;
        if (_status != other._status)
            return false;
        if (_uuid == null) {
            if (other._uuid != null)
                return false;
        } else if (!_uuid.equals(other._uuid))
            return false;
        return true;
    }
}
