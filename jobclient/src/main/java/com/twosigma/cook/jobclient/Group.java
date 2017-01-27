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

import com.twosigma.cook.jobclient.HostPlacement;
import com.twosigma.cook.jobclient.StragglerHandling;


import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.base.Preconditions;

/**
 * An immutable group implementation.
 * <p>
 * A group consists of a set of jobs.
 * Created: November 17, 2016
 *
 * @author diego
 */
final public class Group {
    /**
     * A group status is assigned for the internal functioning of this jobclient.
     * INITIALIZED is the state of any {@link Group} that has been created by this jobclient package,
     * but has not been yet synchronized with the server.
     * WAITING is the state of any {@link Group} for which all jobs are in state WAITING.
     * RUNNING is the state of any {@link Group} for which one or more jobs are in state RUNNING. 
     * COMPLETED is the state of any {@link Group} for which all jobs are in state COMPLETED.
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
     * Group builder
     */
    public static class Builder
    {
        private UUID _uuid;
        private Status _status;
        private String _name;
        private HostPlacement _hostPlacement;
        private StragglerHandling _stragglerHandling;
        private List<UUID> _jobs = new ArrayList<>();

        /**
         * If the group UUID is not provided, the group will be assigned a random UUID, the name cookgroup and a
         * host placement of type "all".<br>
         *
         * @return a instance of {@link Group}.
         */
        public Group build() {

            // set the default values
            if (_uuid == null)
                _uuid = UUID.randomUUID();
            if (_status == null)
                _status = Status.INITIALIZED;
            if (_name == null)
                _name = "cookgroup";
            if (_hostPlacement == null) {
                HostPlacement.Builder hpBuilder = new HostPlacement.Builder();
                _hostPlacement = hpBuilder.setType(HostPlacement.Type.ALL).build();
            }
            if (_stragglerHandling == null) {
                _stragglerHandling = new StragglerHandling.Builder().build();
            }
            return new Group(_uuid, _status, _name, _hostPlacement, _stragglerHandling, _jobs);
        }

        /**
         * Set the UUID of the group to be built.
         *
         * @param uuid {@link UUID} specifies the unique identifier for a group.
         * @return this builder.
         */
        public Builder setUUID(UUID uuid) {
            _uuid = uuid;
            return this;
        }


        /**
         * Set the status of the group to be built.
         *
         * @param status {@link Status} specifies the status of the group to be built.
         * @return this builder.
         */
        public Builder setStatus(Status status) {
            _status = status;
            return this;
        }


        /**
         * Set the name of the group expected to build.
         *
         * @param name {@link String} specifies the name for a group.
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
         * Set the host placement of the group.
         *
         * @param hostPlacement {@link HostPlacement} of the group
         * @return this builder.
         */
        public Builder setHostPlacement(HostPlacement hostPlacement) {
            _hostPlacement = hostPlacement;
            return this;
        }

        /**
         * Set the straggler handling of the group
         *
         * @param stragglerHandling {@link StragglerHandling} of the group
         * @return this builder
         */
        public Builder setStragglerHandling(StragglerHandling stragglerHandling){
            _stragglerHandling = stragglerHandling;
            return this;
        }

        /**
         * Set the jobs of the group to be built.
         *
         * @param jobs {@link Job} to belong to the group.
         * @return this builder.
         */
        private Builder _setJobs(List<UUID> juuids) {
            _jobs = new ArrayList<UUID>(juuids);
            return this;
        }

        /**
         * Add a job (by UUID) to the group to be built.
         *
         * @param job {@link Job} to belong to the group.
         * @return this builder.
         */
        private Builder _addJobByUUID(UUID juuid) {
            _jobs.add(juuid);
            return this;
        }
    }

    final private UUID _uuid;
    final private Status _status;
    final private String _name;
    final private HostPlacement _hostPlacement;
    final private StragglerHandling _stragglerHandling;
    final private List<UUID> _jobs;

    private Group(UUID uuid, Status status, String name, HostPlacement hostPlacement, 
            StragglerHandling stragglerHandling, List<UUID> jobs) {
        _uuid = uuid;
        _status = status;
        _name = name;
        _hostPlacement = hostPlacement;
        _stragglerHandling = stragglerHandling;
        _jobs = ImmutableList.copyOf(jobs);
    }

    /**
     * @return the group UUID.
     */
    public UUID getUUID() {
        return _uuid;
    }

    /**
     * @return the group status.
     */
    public Status getStatus() {
        return _status;
    }

    /**
     * @return the group name.
     */
    public String getName() {
        return _name;
    }

    /**
     * @return the group HostPlacement.
     */
    public HostPlacement getHostPlacement() {
        return _hostPlacement;
    }

    /**
     * @return the group StragglerHandling
     */ 
    public StragglerHandling getStragglerHandling(){
        return _stragglerHandling;
    }

    /**
     * @return the group's jobs.
     */
    public List<UUID> getJobs() {
        return _jobs;
    }

    /**
     * Convert a group to a JSON object, e.g.
     *
     * <pre>
     * <code>
     * {
     *     "uuid" : "26719da8-394f-44f9-9e6d-8a17500f5109",
     *     "name" : "cookgroup",
     *     "host_placement": {
     *         "type": "all"
     *     },
     *     "straggler_handling": {
     *         "type": "none"
     *      }
     * }
     * </code>
     * </pre>
     *
     * @param group specifies the group to be jsonized.
     * @return a JSON object which represents a group.
     * @throws JSONException
     */
    public static JSONObject jsonizeGroup(Group group)
        throws JSONException {
        final JSONObject object = new JSONObject();
        object.put("uuid", group.getUUID().toString());
        object.put("name", group.getName());
        object.put("host_placement", HostPlacement.jsonize(group.getHostPlacement()));
        object.put("straggler_handling", StragglerHandling.jsonize(group.getStragglerHandling()));
        // Do not jsonize jobs. In the POST request to the scheduler, jobs are assigned to groups by setting a
        // 'group' field in each job.
        return object;
    }

    /**
     * Convert a list of groups to a JSON object, e.g.
     *
     * <pre>
     * <code>
     * {
     *    "groups" : [
     *      {
     *          "uuid" : "26719da8-394f-44f9-9e6d-8a17500f5109",
     *          "name" : "cookgroup",
     *          "host_placement": {
     *              "type": "all"
     *          },
     *          "jobs": [
     *              "b7b52297-a8ba-46ed-aae5-9f57669b13e7",
     *              "c08f4da3-196b-419a-b325-fe535d18e79a"
     *          ]
     *      }
     *    ]
     * }
     * </code>
     * </pre>
     *
     * The converted JSON object could be used for group submission via Cook client.
     *
     * @param groups specifies a collection of groups.
     * @return a JSON object which represent a list groups.
     * @throws JSONException
     */
    public static JSONObject jsonizeGroups(Collection<Group> groups)
        throws JSONException {
        final ArrayList<JSONObject> objects = new ArrayList<JSONObject>();
        for (final Group group : groups) {
            objects.add(jsonizeGroup(group));
        }
        final JSONObject object = new JSONObject();
        object.put("groups", objects);
        return object;
    }

    /**
     * Parse a JSON string representing a list of groups, e.g.
     *
     * <pre>
     * <code>
     * [
     *   {
     *       "uuid" : "26719da8-394f-44f9-9e6d-8a17500f5109",
     *       "name" : "cookgroup",
     *       "host_placement": Groups{
     *           "type": "all"
     *       },
     *       "jobs": [
     *           "b7b52297-a8ba-46ed-aae5-9f57669b13e7",
     *           "c08f4da3-196b-419a-b325-fe535d18e79a"
     *       ]
     *   }
     * ]
     * </code>
     * </pre>
     *
     * @param listOfGroups {@link String} specifies a list of groups.
     * @param decorator specifies an instance decorator expected to decorate instances parsed from JSON string.
     *                  If it is null, it will do nothing with it.
     * @return a list of {@link Group}s.
     * @throws JSONException
     */
    public static List<Group> parseFromJSON(String listOfGroups, InstanceDecorator decorator)
        throws JSONException {
        JSONArray jsonArray = new JSONArray(listOfGroups);
        List<Group> groups = new ArrayList<Group>(jsonArray.length());
        for (int i = 0; i < jsonArray.length(); ++i) {
            JSONObject json = jsonArray.getJSONObject(i);
            JSONArray jobsJson = json.getJSONArray("jobs");
            Builder groupBuilder = new Builder();
            groupBuilder.setUUID(UUID.fromString(json.getString("uuid")));
            if (json.has("name")) {
                groupBuilder.setName(json.getString("name"));
            }
            for (int j = 0; j < jobsJson.length(); j++) {
                groupBuilder._addJobByUUID(UUID.fromString(jobsJson.getString(j)));
            }
            groupBuilder.setHostPlacement(HostPlacement.parseFromJSON(json.getJSONObject("host_placement"), decorator));
            groupBuilder.setStragglerHandling(StragglerHandling.parseFromJSON(json.getJSONObject("straggler_handling"), decorator));
            if (json.isNull("completed")) {
                 groupBuilder.setStatus(Status.INITIALIZED);
             } else if (json.getInt("completed") == jobsJson.length()) {
                 groupBuilder.setStatus(Status.COMPLETED);
             } else if (json.getInt("waiting") == jobsJson.length()) {
                 groupBuilder.setStatus(Status.WAITING);
             } else {
                 groupBuilder.setStatus(Status.RUNNING);
             }
            groups.add(groupBuilder.build());
        }
        return groups;
    }

    /**
     * Similar to {@code List<Group> parseFromJSON(String listOfGroups, InstanceDecorator decorator) with {@code decorator}
     * being {@code null}.
     *
     * @param listOfGroups {@link String} specifies a list of groups.
     * @return a list of {@link Group}s.
     * @throws JSONException
     */
    public static List<Group> parseFromJSON(String listOfGroups) throws JSONException {
        return parseFromJSON(listOfGroups, null);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder(512);
        stringBuilder
            .append("Group [_uuid=" + _uuid + ",_status=" + _status + ", _name=" + _name + ", _hostPlacement="
                    + _hostPlacement + ", _stragglerHandling=" + _stragglerHandling + "]");
        return stringBuilder.toString();
    }

    /**
     * A group is equivalent to another if and only if<br>
     * - their UUIDs are the same<br>
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
        Group other = (Group) obj;
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
