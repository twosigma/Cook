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
import org.json.JSONException;
import org.json.JSONObject;

/**
 * An immutable fetchable Mesos URI.
 *
 * This class represents a resource that Mesos should fetch before launching a job.
 * The resource can optionally be cached, extracted, or made executable ({@code chmod +x}).
 * By default, the resource is treated as an archive, and will be unzipped/untarred/un-whatever-ed.
 *
 * @author dgrnbrg
 */
final public class FetchableURI {

    /**
     * URI builder
     */
    public static class Builder {
        private String _value;
        private boolean _cache = false;
        private boolean _extract = true;
        private boolean _executable = false;

        public FetchableURI build() {
            Preconditions.checkNotNull(_value, "value must be set");
            Preconditions.checkArgument(!(_executable && _extract), "URI cannot be executable and extracted");
            return new FetchableURI(_value, _cache, _extract, _executable);
        }

        /**
         * Intialize this builder from the given {@link FetchableURI}
         *
         * @param uri {@link FetchableURI} specifies the uri to copy from
         * @return this builder
         */
        public Builder of(FetchableURI uri) {
            setValue(uri.getValue());
            setCache(uri.isCache());
            setExtract(uri.isExtract());
            setExecutable(uri.isExecutable());
            return this;
        }

        /**
         * Set the URI to fetch using the Mesos fetcher.
         *
         * At a minimum, you should be able to fetch from HTTP, FTP, files, and hdfs (if configured).
         *
         * @param uri The uri to fetch from
         * @return this builder
         */
        public Builder setValue(String value) {
            _value = value;
            return this;
        }

        /**
         * Configure whether to cache the given URI.
         *
         * Artifacts are cached by the user and URI; if you'll be changing the data behind the URI,
         * you should make sure to add a unique, changing hash to the URI (aka a slug).
         *
         * @param cache Whether to cache this URI
         * @return this builder
         */
        public Builder setCache(boolean cache) {
            _cache = cache;
            return this;
        }

        /**
         * Configure whether to extract the given URI.
         *
         * Mesos can extract at least .tar.gz, .zip, and .tar.bz2.
         *
         * @param cache Whether to cache this URI
         * @return this builder
         */
        public Builder setExtract(boolean extract) {
            _extract = extract;
            return this;
        }

        /**
         * Configure whether to set the given URI to be executable.
         *
         * Artifacts are cached by the user and URI; if you'll be changing the data behind the URI,
         * you should make sure to add a unique, changing hash to the URI (aka a slug).
         *
         * @param cache Whether to cache this URI
         * @return this builder
         */
        public Builder setExecutable(boolean executable) {
            _executable = executable;
            return this;
        }
    }

    private final String _value;
    private final boolean _cache;
    private final boolean _extract;
    private final boolean _executable;

    private FetchableURI(String value, boolean cache, boolean extract, boolean executable) {
        _value = value;
        _cache = cache;
        _extract = extract;
        _executable = executable;
    }

    /**
     * @return the URI
     */
    public String getValue() {
        return _value;
    }

    /**
     * @return is this URI cached?
     */
    public boolean isCache() {
        return _cache;
    }

    /**
     * @return is this URI executable?
     */
    public boolean isExecutable() {
        return _executable;
    }

    /**
     * @return is this URI going to be extracted?
     */
    public boolean isExtract() {
        return _extract;
    }

    /**
     * Convert a FetchableURI to a JSON object
     *
     * @param uri specifies the URI
     * @return a JSON object which represents the URI
     * @throws JSONException
     */
    public static JSONObject jsonizeUri(FetchableURI uri)
        throws JSONException {
        final JSONObject object = new JSONObject();
        object.put("value", uri.getValue());
        object.put("executable", uri.isExecutable());
        object.put("extract", uri.isExtract());
        object.put("cache", uri.isCache());
        return object;
    }

    /**
     * Parse a JSON object into a URI
     *
     * @param object a JSON object representing a {@link FetchableURI}
     * @return a {@link FetchableURI}
     * @throws JSONException
     */
    public static FetchableURI parseFromJSON(JSONObject object)
        throws JSONException{
        Builder builder = new Builder();
        builder.setValue(object.getString("value"));
        builder.setExtract(object.getBoolean("extract"));
        builder.setExecutable(object.getBoolean("executable"));
        builder.setCache(object.getBoolean("cache"));
        return builder.build();
    }

    @Override
    public String toString() {
        return "FetchableURI [_value=" + _value
            + ", _extract=" + _extract
            + ", _executable=" + _executable
            + ", _cache=" + _cache
            + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 37;
        int result = 1;
        result = prime * result + _value.hashCode();
        result = prime * result + (_extract ? 0 : 1);
        result = prime * result + (_executable ? 0 : 3);
        result = prime * result + (_cache ? 0 : 5);
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
        FetchableURI other = (FetchableURI) obj;
        if (_extract != other._extract)
            return false;
        if (_executable != other._executable)
            return false;
        if (_cache != other._cache)
            return false;
        if (!_value.equals(other._value))
            return false;
        return true;
    }
}
