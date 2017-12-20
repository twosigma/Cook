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
import java.util.List;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.base.Preconditions;

/**
 * An immutable job instance implementation.
 * <p>
 * An instance of a {@link Job} is a realization of the job. The transition graph of a instance
 * status is strictly UNKNOWN -> RUNNING -> [SUCCESS or FAILED] where<br>
 * -- UNKNOWN specifies a instance status that the instance has been created or initialized and
 * submitted to Mesos <br>
 * -- RUNNING specifies an instance status that the instance has been running at Mesos <br>
 * -- SUCCESS specifies an instance status that the instance has been successfully completed<br>
 * -- FAILED specifies an instance status that the instance has been failed.
 * <p>
 * Created: March 14, 2015
 *
 * @author wzhao
 */
final public class Instance {
    /**
     * An instance in Cook scheduler only has four statuses: UNKNOWN, RUNNING, SUCCESS, and FAILED.
     */
    public static enum Status {
        UNKNOWN("UNKNOWN"), RUNNING("RUNNING"), SUCCESS("SUCCESS"), FAILED("FAILED");

        Status(String name) {}

        /**
         * @param name specifies a string representation of status.
         * @return a constant for the specified name.
         */
        public static Status fromString(String name) {
            return Enum.valueOf(Status.class, name.trim().toUpperCase());
        }
    }

    /**
     * Instance builder
     */
    public static class Builder {
        private UUID _taskID;
        private String _slaveID;
        private String _executorID;
        private Long _startTime;
        private Long _endTime;
        private Status _status;
        private Integer _progress;
        private String _progressMessage;
        private Long _reasonCode;
        private Boolean _preempted;
        private String _outputURL;
        private String _hostName;
        private Executor _executor;

        /**
         * The task id must be provided prior to {@code build()}. If the instance status is not
         * provided, it will use UNKNOWN as the default status.
         *
         * @return an instance of job instance.
         */
        public Instance build() {
            Preconditions.checkNotNull(_taskID, "TaskID can not be null!");
            if (_status == null) {
                _status = Status.UNKNOWN;
            }
            return new Instance(_taskID, _slaveID, _executorID, _startTime, _endTime, _status, _progress,
                _progressMessage,  _reasonCode, _preempted, _outputURL, _hostName, _executor);
        }

        /**
         * Set the unique identifier for the task expected to build.
         *
         * @param uuid {@link UUID} specifies the UUID of a task.
         * @return this builder.
         */
        public Builder setTaskID(UUID uuid) {
            Preconditions.checkNotNull(uuid, "TaskID can not be null!");
            _taskID = uuid;
            return this;
        }

        /**
         * Set the slave ID for the task expected to build where a slave ID is the identifier for
         * the Mesos slave which the task is assigned to.
         *
         * @param slaveID {@link String} specifies the slave ID which the task is assign to.
         * @return this builder.
         */
        public Builder setSlaveID(String slaveID) {
            _slaveID = slaveID;
            return this;
        }

        /**
         * Set the Mesos executor ID for the task expected to build.
         *
         * @param executorID {@link String} specifies the executor ID.
         * @return this builder.
         */
        public Builder setExecutorID(String executorID) {
            _executorID = executorID;
            return this;
        }

        /**
         * Set the the starting time in milliseconds for the task expected to build.
         *
         * @param startTime {@link Long} specifies the starting time in milliseconds.
         * @return this builder.
         */
        public Builder setStartTime(Long startTime) {
            _startTime = startTime;
            return this;
        }

        /**
         * Set the the ending time in milliseconds for the task expected to build.
         *
         * @param endTime {@link Long} specifies the ending time in milliseconds.
         * @return this builder.
         */
        public Builder setEndTime(Long endTime) {
            _endTime = endTime;
            return this;
        }

        /**
         * Set the the task status for the task expected to build.
         *
         * @param status {@link Status} specifies the task status.
         * @return this builder.
         */
        public Builder setStatus(Status status) {
            _status = status;
            return this;
        }

        /**
         * Set the the task progress percent for the task expected to build.
         *
         * @param progress {@link Integer} specifies the task progress percent.
         * @return this builder.
         */
        public Builder setProgress(Integer progress) {
            _progress = progress;
            return this;
        }

        /**
         * Set the the task progress message for the task expected to build.
         *
         * @param progressMessage {@link String} specifies the task progress message.
         * @return this builder.
         */
        public Builder setProgressMessage(String progressMessage) {
            _progressMessage = progressMessage;
            return this;
        }

        public Builder setReasonCode(Long reasonCode) {
            _reasonCode = reasonCode;
            return this;
        }

        /**
         * Sets whether the instance was preempted.
         *
         * @param preempted true if this Instance was preempted, false otherwise
         * @return this builder.
         */
        public Builder setPreempted(Boolean preempted) {
            _preempted = preempted;
            return this;
        }

        /**
         * Set the task local output directory for the task expected to build.
         *
         * @param outputURL {@link String} specifies the task local output directory in the Mesos
         *        host.
         * @return this builder.
         */
        public Builder setOutputURL(String outputURL) {
            _outputURL = outputURL;
            return this;
        }

        /**
         * Set the Mesos host for the task expected to build.
         *
         * @param hostName {@link String} specifies the host where this task is assigned to.
         * @return this builder.
         */
        public Builder setHostName(String hostName) {
            _hostName = hostName;
            return this;
        }

        /**
         * Set the executor of the instance expected to build.
         *
         * @param executor {@link String} specifies executor for a job.
         * @return this builder.
         */
        public Builder setExecutor(String executor) {
            return setExecutor(Executor.fromString(executor));
        }

        /**
         * Set the executor of the instance expected to build.
         *
         * @param executor {@link Executor} specifies executor for a job.
         * @return this builder.
         */
        public Builder setExecutor(Executor executor) {
            _executor = executor;
            return this;
        }

        public UUID getTaskID() {
            return _taskID;
        }

        public String getSlaveID() {
            return _slaveID;
        }

        public String getExecutorID() {
            return _executorID;
        }

        public Long getStartTime() {
            return _startTime;
        }

        public Long getEndTime() {
            return _endTime;
        }

        public Status getStatus() {
            return _status;
        }

        /**
         * @return the progress percent of the instance. It returns null if the progress percent is unavailable.
         */
        public Integer getProgress() {
            return _progress;
        }

        /**
         * @return the progress message associated with the instance. It returns null if the message is unavailable.
         */
        public String getProgressMessage() {
            return _progressMessage;
        }


        public String getOutputURL() {
            return _outputURL;
        }

        public String getHostName() {
            return _hostName;
        }

        public Executor getExecutor() {
            return _executor;
        }
    }

    /**
     * Depending on an instance's status, some of the following fields could be null.
     */
    final private UUID _taskID;
    final private String _slaveID;
    final private String _executorID;
    final private Long _startTime;
    final private Long _endTime;
    final private Status _status;
    final private Integer _progress;
    final private String _progressMessage;
    final private Long _reasonCode;
    final private Boolean _preempted;
    final private String _outputURL;
    final private String _hostName;
    final private Executor _executor;

    private Instance(UUID taskID, String slaveID, String executorID, Long startTime, Long endTime,
                     Status status, Integer progress, String progressMessage, Long reasonCode, Boolean preempted,
                     String outputURL, String hostName, Executor executor) {
        _taskID = taskID;
        _slaveID = slaveID;
        _executorID = executorID;
        _startTime = startTime;
        _endTime = endTime;
        _status = status;
        _progress = progress;
        _progressMessage = progressMessage;
        _reasonCode = reasonCode;
        _preempted = preempted;
        _outputURL = outputURL;
        _hostName = hostName;
        _executor = executor;
    }

    /**
     * Parse a JSON string representing a list of {@link Instance}s, e.g.
     *
     * <pre>
     * <code>
     * [
     *   {
     *      "slave_id" : "20150311-033720-1963923116-5050-4084-32",
     *      "end_time" : 1426632251828,
     *      "status" : "success",
     *      "progress" : 20,
     *      "progress_message" : "twenty percent done",
     *      "start_time" : 1426632249597,
     *      "hostname" : "server1.example.com",
     *      "executor" : "mesos",
     *      "executor_id" : "f52fbacf-52a1-44a2-bda1-cbfa477cc163",
     *      "task_id" : "f52fbacf-52a1-44a2-bda1-cbfa477cc163",
     *      "preempted": false
     *   }
     * ]
     * </code>
     * </pre>
     *
     * @param listOfInstances {@link String} specifies a string representing a list of
     *        {@link Instance}s.
     * @return a list of {@link Instance}s.
     * @throws JSONException
     */
    public static List<Instance> parseFromJSON(String listOfInstances) throws JSONException {
        return parseFromJSON(new JSONArray(listOfInstances));
    }

    /**
     * Parse a JSON array representing a list of {@link Instance}.
     *
     * @param listOfInstances specifies a JSON array representing a list of {@link Instance}s.
     * @param decorator       specifies {@link InstanceDecorator} expected to decorate instances parsed from JSON.
     *                        If it is {@code null}, it will do nothing.
     * @return a list of {@link Instance}s.
     * @throws JSONException
     */
    public static List<Instance> parseFromJSON(JSONArray listOfInstances, InstanceDecorator decorator)
            throws JSONException {
        final List<Instance> instances = new ArrayList<>(listOfInstances.length());
        for (int i = 0; i < listOfInstances.length(); ++i) {
            JSONObject json = listOfInstances.getJSONObject(i);
            Builder instanceBuilder = new Builder();
            instanceBuilder.setTaskID(UUID.fromString(json.getString("task_id")));
            instanceBuilder.setSlaveID(json.getString("slave_id"));
            instanceBuilder.setExecutorID(json.getString("executor_id"));
            instanceBuilder.setHostName(json.getString("hostname"));
            if (json.has("executor")) {
                instanceBuilder.setExecutor(json.getString("executor"));
            }
            instanceBuilder.setStatus(Status.fromString(json.getString("status")));
            if (json.has("progress")) {
                instanceBuilder.setProgress(json.getInt("progress"));
            }
            if (json.has("progress_message")) {
                instanceBuilder.setProgressMessage(json.getString("progress_message"));
            }
            instanceBuilder.setPreempted(json.getBoolean("preempted"));
            instanceBuilder.setStartTime(json.getLong("start_time"));
            if (json.has("end_time")) {
                instanceBuilder.setEndTime(json.getLong("end_time"));
            }
            if (json.has("output_url")) {
                instanceBuilder.setOutputURL(json.getString("output_url"));
            }
            if (json.has("reason_code")) {
                instanceBuilder.setReasonCode(json.getLong("reason_code"));
            }
            if (decorator != null) {
                instanceBuilder = decorator.decorate(instanceBuilder);
            }
            Instance instance = instanceBuilder.build();
            instances.add(instance);
        }
        return instances;
    }

    /**
     * Similar to {@code List<Instance> parseFromJSON(JSONArray listOfInstances, InstanceDecorator decorator)} with
     * {@code decorator} being null.
     *
     * @param listOfInstances specifies a JSON array representing a list of {@link Instance}s.
     * @return a list of {@link Instance}s.
     * @throws JSONException
     */
    public static List<Instance> parseFromJSON(JSONArray listOfInstances)
            throws JSONException {
        return parseFromJSON(listOfInstances, null);
    }

    @Override
    public String toString() {
        return "Instance [_taskID=" + _taskID + ", _slaveID=" + _slaveID + ", _executorID="
                + _executorID + ", _startTime=" + _startTime + ", _endTime=" + _endTime
                + ", _status=" + _status + ", _progress=" + _progress + ", _progressMessage=" + _progressMessage
                + ", _reasonCode=" + _reasonCode + ", _preempted=" + _preempted + ", _outputURL=" + _outputURL
                + ", _hostName=" + _hostName + ", _executor=" + _executor + "]";
    }

    public UUID getTaskID() {
        return _taskID;
    }

    public String getSlaveID() {
        return _slaveID;
    }

    public String getExecutorID() {
        return _executorID;
    }

    public Long getStartTime() {
        return _startTime;
    }

    public Long getEndTime() {
        return _endTime;
    }

    public Status getStatus() {
        return _status;
    }

    /**
     * @return the progress percent of the instance. It returns null if the progress percent is unavailable.
     */
    public Integer getProgress() {
        return _progress;
    }

    /**
     * @return the progress message associated with the instance. It returns null if the message is unavailable.
     */
    public String getProgressMessage() {
        return _progressMessage;
    }

    public Long getReasonCode() {
        return _reasonCode;
    }

    public Boolean getPreempted() {
        return _preempted;
    }

    public String getOutputURL() {
        return _outputURL;
    }

    public String getHostName() {
        return _hostName;
    }

    public Executor getExecutor() {
        return _executor;
    }


    /**
     * An instance equals to another one if and only if<br>
     * - their task ids are the same<br>
     * - their task statuses are the same<br>
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_status == null) ? 0 : _status.hashCode());
        result = prime * result + ((_taskID == null) ? 0 : _taskID.hashCode());
        return result;
    }

    /**
     * An instance equals to another one if and only if<br>
     * - their task ids are the same<br>
     * - their task statuses are the same<br>
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!getClass().equals(obj.getClass()))
            return false;
        Instance other = (Instance) obj;
        if (_status != other._status)
            return false;
        if (_taskID == null) {
            if (other._taskID != null)
                return false;
        } else if (!_taskID.equals(other._taskID))
            return false;
        return true;
    }
}
