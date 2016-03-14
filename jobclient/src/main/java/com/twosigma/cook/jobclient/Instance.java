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
     * Instance builder
     */
    public static class Builder {
        private UUID _taskID;
        private String _slaveID;
        private String _executorID;
        private Long _startTime;
        private Long _endTime;
        private Status _status;
        private Long _reasonCode;
        private String _outputURL;
        private String _hostName;

        /**
         * The task id must be provided prior to {@code build()}. If the instance status is not
         * provided, it will use UNKNOWN as the default status.
         *
         * @return an instance of job instance.
         */
        public Instance build() {
            Preconditions.checkNotNull(_taskID, "TaskID can not be null!");
            if (_status == null)
                _status = Status.UNKNOWN;
            return new Instance(_taskID, _slaveID, _executorID, _startTime, _endTime, _status, _reasonCode,
                    _outputURL, _hostName);
        }

        /**
         * Set the unique identifier for the task expected to build.
         *
         * @param uuid {@link UUID} specifies the UUID of a task.
         * @return this builder.
         */
        public Builder setTaskID(UUID uuid) {
            Preconditions.checkNotNull(uuid, "TaskID can not be null!");
            Preconditions.checkState(_taskID == null, "Task ID has been set!");
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
            Preconditions.checkState(_slaveID == null, "Slave ID has been set!");
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
            Preconditions.checkState(_executorID == null, "Executor ID has been set!");
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
            Preconditions.checkState(_startTime == null, "Start time has been set!");
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
            Preconditions.checkState(_endTime == null, "End time has been set!");
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
            Preconditions.checkState(_status == null, "Status has been set!");
            _status = status;
            return this;
        }

        public Builder setReasonCode(Long reasonCode) {
            Preconditions.checkState(_reasonCode == null, "Reason code has been set!");
            _reasonCode = reasonCode;
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
            Preconditions.checkState(_outputURL == null, "Output URL has been set!");
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
            Preconditions.checkState(_hostName == null, "Host name URL has been set!");
            _hostName = hostName;
            return this;
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
    final private Long _reasonCode;
    final private String _outputURL;
    final private String _hostName;

    private Instance(UUID taskID, String slaveID, String executorID, Long startTime, Long endTime,
                     Status status, Long reasonCode, String outputURL, String hostName) {
        _taskID = taskID;
        _slaveID = slaveID;
        _executorID = executorID;
        _startTime = startTime;
        _endTime = endTime;
        _status = status;
        _reasonCode = reasonCode;
        _outputURL = outputURL;
        _hostName = hostName;
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
     *      "start_time" : 1426632249597,
     *      "hostname" : "simfarm73.dft.twosigma.com",
     *      "executor_id" : "f52fbacf-52a1-44a2-bda1-cbfa477cc163",
     *      "task_id" : "f52fbacf-52a1-44a2-bda1-cbfa477cc163"
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
     * @return a list of {@link Instance}s.
     * @throws JSONException
     */
    public static List<Instance> parseFromJSON(JSONArray listOfInstances) throws JSONException {
        List<Instance> instances = new ArrayList<Instance>(listOfInstances.length());
        for (int i = 0; i < listOfInstances.length(); ++i) {
            JSONObject json = listOfInstances.getJSONObject(i);
            Builder instanceBuilder = new Builder();
            instanceBuilder.setTaskID(UUID.fromString(json.getString("task_id")));
            instanceBuilder.setSlaveID(json.getString("slave_id"));
            instanceBuilder.setExecutorID(json.getString("executor_id"));
            instanceBuilder.setHostName(json.getString("hostname"));
            instanceBuilder.setStatus(Status.fromString(json.getString("status")));
            instanceBuilder.setStartTime(json.getLong("start_time"));
            if (json.has("end_time"))
                instanceBuilder.setEndTime(json.getLong("end_time"));
            if (json.has("output_url"))
                instanceBuilder.setOutputURL(json.getString("output_url"));
            if (json.has("reason_code"))
                instanceBuilder.setReasonCode(json.getLong("reason_code"));

            Instance instance = instanceBuilder.build();
            instances.add(instance);
        }
        return instances;
    }

    @Override
    public String toString() {
        return "Instance [_taskID=" + _taskID + ", _slaveID=" + _slaveID + ", _executorID="
                + _executorID + ", _startTime=" + _startTime + ", _endTime=" + _endTime
                + ", _status=" + _status + ", _reasonCode=" + _reasonCode + ", _outputURL=" + _outputURL + ", _hostName="
                + _hostName + "]";
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

    public Long getReasonCode() {
        return _reasonCode;
    }

    public String getOutputURL() {
        return _outputURL;
    }

    public String getHostName() {
        return _hostName;
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
