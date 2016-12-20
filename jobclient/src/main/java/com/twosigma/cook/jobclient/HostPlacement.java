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

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.ImmutableMap;
import com.google.common.base.Preconditions;

/**
 * An immutable host-placement implementation.
 * <p>
 * A HostPlacement has a type and named parameters.
 * Created: November 17, 2016
 *
 * @author diego
 */
final public class HostPlacement {
    /**
     * There are five types of host placement: UNIQUE, BALANCED, ONE, ATTRIBUTE_EQUALS and ALL.
     */
    public static enum Type {
        UNIQUE("UNIQUE"), BALANCED("BALANCED"), ONE("ONE"), ATTRIBUTE_EQUALS("ATTRIBUTE-EQUALS"),
            ALL("ALL");

        private Type(String name) {}

        /**
         * @param name specifies a string representation of status.
         * @return a constant for the specified name.
         */
        public static Type fromString(String name) {
            return Enum.valueOf(Type.class, name.trim().toUpperCase());
        }
    }
    /**
     * HostPlacement builder
     */
    public static class Builder
    {
        private Type _type;
        private Map<String,String> _parameters = new HashMap<>();

        /**
         * Prior to {@code build()}, type must be set.<br>
         * - If the type is set to ATTRIBUTE_EQUALS, then a parameter "attribute" must exist.<br>
         *
         * @return a instance of {@link HostPlacement}.
         */
        public HostPlacement build() {
            if (_type == null) {
                _type = Type.fromString("ALL");
            } else if (_type == Type.ATTRIBUTE_EQUALS) {
                Preconditions.checkNotNull(_parameters.get("attribute"), "attribute parameter is required with " +
                        "type attribute-equals" );
            }

            // set the default values
            return new HostPlacement(_type, _parameters);
        }

        /**
         * Set command, memory, cpus, env vars, uris, and retries from a job.
         *
         * @param job {@link Job} specifies a job.
         * @return this builder.
         */
        public Builder of(HostPlacement hp) {
            setType(hp.getType());
            setParameters(hp.getParameters());
            return this;
        }

        /**
         * Set the HostPlacement type
         *
         * @param type The type of host placement
         * @return this builder
         */
        public Builder setType(Type type) {
            _type = type;
            return this;
        }

        /**
         * Set the HostPlacement parameters
         *
         * @param parameters The HostPlacement parameters
         * @return this builder
         */
        public Builder setParameters(Map<String,String> parameters) {
            _parameters = ImmutableMap.copyOf(parameters);
            return this;
        }

        /**
         * Add a HostPlacement parameter
         *
         * @param name The name of the parameter
         * @param val The value of the parameter
         * @return this builder
         */
        public Builder setParameter(String name, String val) {
            _parameters.put(name, val);
            return this;
        }

    }

    final private Type _type;
    final private Map<String,String> _parameters;

    private HostPlacement(Type type, Map<String,String> parameters) {
        _type = type;
        _parameters = ImmutableMap.copyOf(parameters);
    }

    /**
     * @return the HostPlacement type
     */
    public Type getType() {
        return _type;
    }

    /**
     * @return the HostPlacement parameters
     */
    public Map<String,String> getParameters() {
        return _parameters;
    }

    /**
     * @param name The name of a HostPlacement parameter
     * @return The value of a HostPlacement parameter, or null if it does not exist
     */
    public String getParameters(String name) {
        return _parameters.get(name);
    }

    /**
     * Convert a job to a JSON object, e.g.
     *
     * <pre>
     * <code>
     * {
     *     "type": "attribute-equals",
     *     "parameters": {
     *         "attribute": "test"
     *     }
     * }
     * </code>
     * </pre>
     *
     * @param hp specifies a HostPlacement.
     * @return a JSON object which represents a HostPlacement.
     * @throws JSONException
     */
    public static JSONObject jsonize(HostPlacement hp)
        throws JSONException {
        final JSONObject object = new JSONObject();
        object.put("type", hp.getType().toString().toLowerCase());
        object.put("parameters", hp.getParameters());
        return object;
    }

    public static HostPlacement parseFromJSON(JSONObject hpJson, InstanceDecorator decorator)
        throws JSONException {
        Builder hpBuilder = new Builder();

        String type = hpJson.getString("type");
        hpBuilder.setType(Type.fromString(type.substring(type.lastIndexOf("/") + 1).toUpperCase()));
        JSONObject paramJson = hpJson.getJSONObject("parameters");
        Iterator names = paramJson.keys();
        while (names.hasNext()) {
            String name = (String)names.next();
            hpBuilder.setParameter(name, paramJson.getString(name));
        }
        return hpBuilder.build();
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder(512);
        stringBuilder
            .append("HostPlacement [_type=" + _type + ", _parameters=" + _parameters.toString() + "]");
        stringBuilder.append('\n');
        return stringBuilder.toString();
    }
}
