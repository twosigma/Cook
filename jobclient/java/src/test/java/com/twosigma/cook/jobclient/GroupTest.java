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

import java.util.List;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link Group}.
 * 
 * @author diego
 */
public class GroupTest {

    /*
     * A Group which could be used for any test.
     */
    private Group _initializedGroup;

    @Before
    public void setup() {
        final Group.Builder groupBuilder = new Group.Builder();
        groupBuilder.setUUID(UUID.randomUUID());
        _initializedGroup = groupBuilder.build();
    }

    @Test
    public void testJsonizeGroup() throws JSONException {
        final JSONObject jsonGroup = Group.jsonizeGroup(_initializedGroup);
        Assert.assertEquals(jsonGroup.getString("uuid"), _initializedGroup.getUUID().toString());
    }

    @Test
    public void testParseFromJSON() throws JSONException {
        final JSONObject json = Group.jsonizeGroup(_initializedGroup);
        json.put("jobs", new JSONArray().put(UUID.randomUUID().toString()));
        final String jsonString = new JSONArray().put(json).toString();
        final List<Group> groups = Group.parseFromJSON(jsonString);
        Assert.assertEquals(groups.size(), 1);
        Assert.assertEquals(groups.get(0), _initializedGroup);
    }
}

