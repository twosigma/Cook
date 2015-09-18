
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

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link FetchableURI}.
 * 
 * @author dgrnbrg
 */
public class FetchableURITest {

    private FetchableURI _uri;

    @Before
    public void setup() {
        final FetchableURI.Builder builder = new FetchableURI.Builder();
        builder.setValue("http://example.com/myresource.sh");
        builder.setExecutable(true);
        builder.setExtract(false);
        builder.setCache(true);
        _uri = builder.build();
    }

    @Test
    public void testJsonizeURI() throws JSONException {
        final JSONObject json = FetchableURI.jsonizeUri(_uri);
        Assert.assertEquals(json.getString("value"), _uri.getValue());
        Assert.assertEquals(json.getBoolean("executable"), _uri.isExecutable());
    }

    @Test
    public void testParseFromJSON() throws JSONException {
        final JSONObject json = FetchableURI.jsonizeUri(_uri);
        Assert.assertEquals(FetchableURI.parseFromJSON(json), _uri);
    }
}

