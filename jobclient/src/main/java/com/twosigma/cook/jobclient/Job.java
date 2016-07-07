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

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

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

        private Status(String name) {}

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
    public static class Builder
    {
        private UUID _uuid;
        private String _name;
        private String _command;
        private Double _memory;
        private Double _cpus;
        private Integer _retries;
        private Status _status;
        private Integer _priority;
        private List<Instance> _instances = Collections.synchronizedList(new ArrayList<Instance>());
        private List<FetchableURI> _uris = new ArrayList<>();
        private Map<String,String> _env = new HashMap<>();
        private JSONObject _container;
        private Map<String,String> _labels = new HashMap<>();

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

            // set the default values
            if (_uuid == null)
                _uuid = UUID.randomUUID();
            if (_retries == null)
                _retries = 5;
            if (_priority == null)
                _priority = 50;
            if (_status == null)
                _status = Status.INITIALIZED;
            if (_name == null)
                _name = "cookjob";
            return new Job(_uuid, _name, _command, _memory, _cpus, _retries, _status, _priority, _instances, _env, _uris, _container, _labels);
        }

        /**
         * Set command, memory, cpus, env vars, uris, and retries from a job.
         *
         * @param job {@link Job} specifies a job.
         * @return this builder.
         */
        public Builder of(Job job) {
            setCommand(job.getCommand());
            setMemory(job.getMemory());
            setCpus(job.getCpus());
            setRetries(job.getRetries());
            setEnv(job.getEnv());
            setUris(job.getUris());
            setContainer(job.getContainer());
            setLabels(job.getLabels());
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
         *
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
         * @param name specifies the name of the env var
         * @param value specifies the value of the env var
         * @return this builder
         */
        public Builder addEnv(String name, String value) {
            _env.put(name, value);
            return this;
        }

        /**
         * Adds a collection of env vars to the job.
         *
         * This adds the enviroment to the job; it won't remove variables that were previously set.
         *
         * @param environment specifies the environment to add to this job.
         * @return this builder
         */
        public Builder addEnv(Map<String,String> environment) {
            _env.putAll(environment);
            return this;
        }

        /**
         * Adds a collection of env vars to the job.
         *
         * This resets the enviroment to the job; it will remove variables that were previously set.
         *
         * @param environment specifies the environment to set for this job.
         * @return this builder
         */
        public Builder setEnv(Map<String,String> environment) {
            _env = ImmutableMap.copyOf(environment);
            return this;
        }

        /**
         * Add an label to the job
         *
         * @param key specifies the key of the label
         * @param value specifies the value of the label
         * @return this builder
         */
        public Builder addLabel(String key, String value) {
            _labels.put(key, value);
            return this;
        }

        /**
         * Adds a collection of labels to the job.
         *
         * This adds the labels to the job; it won't remove labels that were previously set.
         *
         * @param labels specifies the labelironment to add to this job.
         * @return this builder
         */
        public Builder addLabels(Map<String,String> labels) {
            _labels.putAll(labels);
            return this;
        }

        /**
         * Adds a collection of label to the job.
         *
         * This resets the labels of the job; it will remove labels that were previously set.
         *
         * @param labels specifies the labels to set for this job.
         * @return this builder
         */
        public Builder setLabels(Map<String,String> labels) {
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
         * @param name {@link Status} specifies the name for a job.
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
        public Builder setPriority(Integer priority){
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
    }

    final private UUID _uuid;
    final private String _name;
    final private String _command;
    final private Double _memory;
    final private Double _cpus;
    final private Integer _retries;
    final private Integer _priority;
    final private Status _status;
    final private List<Instance> _instances;
    final private Map<String, String> _env;
    final private List<FetchableURI> _uris;
    final private JSONObject _container;
    final private Map<String, String> _labels;

    private Job(UUID uuid, String name, String command, Double memory, Double cpus, Integer retries, Status status,
                Integer priority, List<Instance> instances, Map<String,String> env, List<FetchableURI> uris,
                JSONObject container, Map<String,String> labels) {
        _uuid = uuid;
        _name = name;
        _command = command;
        _memory = memory;
        _cpus = cpus;
        _retries = retries;
        _status = status;
        _priority = priority;
        _instances = ImmutableList.copyOf(instances);
        _env = ImmutableMap.copyOf(env);
        _uris = ImmutableList.copyOf(uris);
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
     * @return the job's environment
     */
    public Map<String,String> getEnv() {
        return _env;
    }

    /**
     * @return the job's labels
     */
    public Map<String,String> getLabels() {
        return _labels;
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
    public Integer getPriority(){
        return _priority;
    }

    /**
     * @return the job instances.
     */
    public List<Instance> getInstances() {
        return _instances;
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
     *
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
        object.put("mem", job.getMemory());
        object.put("cpus", job.getCpus());
        object.put("priority", job.getPriority());
        object.put("max_retries", job.getRetries());
        object.put("status", job.getStatus());
        object.put("env", env);
        object.put("labels", labels);
        if(container != null) {
            object.put("container", container);
        }
        for (FetchableURI uri : job.getUris()) {
            object.append("uris", FetchableURI.jsonizeUri(uri));
        }
        return object;
    }

    /**
     * Convert a list of job to a JSON object, e.g.
     *
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
     *
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
     *
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
     *             "hostname" : "simfarm73.dft.twosigma.com",
     *             "executor_id" : "f52fbacf-52a1-44a2-bda1-cbfa477cc163",
     *             "task_id" : "f52fbacf-52a1-44a2-bda1-cbfa477cc163"
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
     * @param decorator specifies an instance decorator expected to decorate instances parsed from JSON string.
     *                  If it is null, it will do nothing with it.
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
            jobBuilder.setPriority(json.getInt("priority"));
            jobBuilder.setStatus(Status.fromString(json.getString("status")));
            if (json.has("name")) {
                jobBuilder.setName(json.getString("name"));
            }
            jobBuilder.setRetries(json.getInt("max_retries"));
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
            jobBuilder.addInstances(Instance.parseFromJSON(json.getJSONArray("instances"), decorator));
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
            .append("Job [_uuid=" + _uuid + ", _name=" + _name + ", _command=" + _command + ", _memory=" + _memory
                    + ", _cpus=" + _cpus + ", _retries=" + _retries + ", _status=" + _status + ", _priority="
                    + _priority + "]");
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
