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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An immutable straggler handling implementation.
 * <p>
 * A StragglerHandling has a type and named parameters.
 * Created: November 17, 2016
 *
 * @author wyegelwel
 */
final public class StragglerHandling {
    public enum Type {
        NONE("NONE"), QUANTILE_DEVIATION("QUANTILE-DEVIATION");

        private final String name;

        // Reverse-lookup map for getting a Type from its name
        private static final Map<String, Type> lookup = new HashMap<>();

        static {
            for (Type type : Type.values()) {
                lookup.put(type.name, type);
            }
        }

        Type(String name) {
            this.name = name;
        }

        /**
         * @param name specifies a string representation of status.
         * @return a constant for the specified name.
         */
        public static Type fromString(String name) {
            return lookup.get(name);
        }

        @Override
        public String toString() {
            return this.name;
        }
    }

    /**
     * StragglerHandling builder
     */
    public static class Builder {
        private Type _type;
        private Map<String, Object> _parameters = new HashMap<>();

        /**
         * Prior to {@code build()}, type must be set.<br>
         * - If the type is set to QUANTILE_DEVIATION, then parameters "quantile" and
         * "multiplier"  must exist.<br>
         *
         * @return a instance of {@link StragglerHandling}.
         */
        public StragglerHandling build() {
            if (_type == null) {
                _type = Type.fromString("NONE");
            } else if (_type == Type.QUANTILE_DEVIATION) {
                Preconditions.checkNotNull(_parameters.get("quantile"),
                        "quantile parameter is required with type quantile-deviation");
                Preconditions.checkNotNull(_parameters.get("multiplier"),
                        "multiplier parameter is required with type quantile-deviation");
            }

            // set the default values
            return new StragglerHandling(_type, _parameters);
        }

        /**
         * Set command, memory, cpus, env vars, uris, and retries from a job.
         *
         * @param sh {@link StragglerHandling} specifies a straggler-handling object.
         * @return this builder.
         */
        public Builder of(StragglerHandling sh) {
            setType(sh.getType());
            setParameters(sh.getParameters());
            return this;
        }

        /**
         * Set the StragglerHandling type
         *
         * @param type The type of straggler handling
         * @return this builder
         */
        public Builder setType(Type type) {
            _type = type;
            return this;
        }

        /**
         * Set the StragglerHandling parameters
         *
         * @param parameters The StragglerHandling parameters
         * @return this builder
         */
        public Builder setParameters(Map<String, Object> parameters) {
            _parameters = ImmutableMap.copyOf(parameters);
            return this;
        }

        /**
         * Add a StragglerHandling parameter
         *
         * @param name The name of the parameter
         * @param val  The value of the parameter
         * @return this builder
         */
        public Builder setParameter(String name, Object val) {
            _parameters.put(name, val);
            return this;
        }

    }

    final private Type _type;
    final private Map<String, Object> _parameters;

    private StragglerHandling(Type type, Map<String, Object> parameters) {
        _type = type;
        _parameters = ImmutableMap.copyOf(parameters);
    }

    /**
     * @return the StragglerHandling type
     */
    public Type getType() {
        return _type;
    }

    /**
     * @return the StragglerHandling parameters
     */
    public Map<String, Object> getParameters() {
        return _parameters;
    }

    /**
     * @param name The name of a StragglerHandling parameter
     * @return The value of a StragglerHandling parameter, or null if it does not exist
     */
    public Object getParameter(String name) {
        return _parameters.get(name);
    }

    /**
     * Convert a StragglerHandling to a JSON object, e.g.
     *
     * <pre>
     * <code>
     * {
     *     "type": "quantile-deviation",
     *     "parameters": {
     *         "quantile": 0.5,
     *         "multiplier": 2.5
     *     }
     * }
     * </code>
     * </pre>
     *
     * @param sh specifies a StragglerHandling.
     * @return a JSON object which represents a StragglerHandling.
     * @throws JSONException
     */
    public static JSONObject jsonize(StragglerHandling sh)
            throws JSONException {
        final JSONObject object = new JSONObject();
        object.put("type", sh.getType().toString().toLowerCase());
        object.put("parameters", sh.getParameters());
        return object;
    }

    public static StragglerHandling parseFromJSON(JSONObject shJson, InstanceDecorator decorator)
            throws JSONException {
        Builder shBuilder = new Builder();

        String type = shJson.getString("type");
        shBuilder.setType(Type.fromString(type.toUpperCase()));
        if (shJson.has("parameters")) {
            JSONObject paramJson = shJson.getJSONObject("parameters");
            Iterator names = paramJson.keys();
            while (names.hasNext()) {
                String name = (String) names.next();
                shBuilder.setParameter(name, paramJson.get(name));
            }
        }
        return shBuilder.build();
    }

    @Override
    public String toString() {
        return ("StragglerHandling [_type=" + _type + ", _parameters=" + _parameters.toString() + "]") + '\n';
    }
}
