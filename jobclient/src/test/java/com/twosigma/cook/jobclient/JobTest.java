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
 * Unit tests for {@link Job}.
 * 
 * @author wzhao
 */
public class JobTest {

    /*
     * A job which could be used for any test.
     */
    private Job _initializedJob;

    @Before
    public void setup() {
        final Job.Builder jobBuilder = new Job.Builder();
        jobBuilder.setUUID(UUID.randomUUID());
        jobBuilder.setCommand("sleep 10s");
        jobBuilder.setMemory(100.0);
        jobBuilder.setCpus(1.0);
        jobBuilder.addEnv("FOO", "test");
        jobBuilder.addLabel("foobar", "frobnicator");
        jobBuilder.setMaxRuntime(1000L);
        jobBuilder.disableMeaCulpaRetries();
        jobBuilder.addUri(new FetchableURI.Builder().setValue("http://example.com/my_resource").build());
        jobBuilder.setApplication(new Application("baz-app", "1.2.3"));
        jobBuilder.setExpectedRuntime(500L);
        _initializedJob = jobBuilder.build();
    }

    @Test
    public void testJsonizeJob() throws JSONException {
        final JSONObject jsonJob = Job.jsonizeJob(_initializedJob);
        Assert.assertEquals(jsonJob.getString("uuid"), _initializedJob.getUUID().toString());
        Assert.assertEquals(
                jsonJob.getJSONObject("application").toString(),
                new JSONObject().put("name", "baz-app").put("version", "1.2.3").toString());
        Assert.assertEquals(500L, jsonJob.getLong("expected_runtime"));
        Assert.assertEquals(true, jsonJob.getBoolean("disable_mea_culpa_retries"));
    }

    @Test
    public void testParseFromJSON() throws JSONException {
        final JSONObject json = Job.jsonizeJob(_initializedJob);
        json.put("instances", new JSONArray());
        json.put("status", "INITIALIZED");
        final String jsonString = new JSONArray().put(json).toString();
        final List<Job> jobs = Job.parseFromJSON(jsonString);
        Assert.assertEquals(jobs.size(), 1);
        Assert.assertEquals(jobs.get(0), _initializedJob);
        Assert.assertEquals(jobs.get(0).getMaxRuntime(), new Long(1000L));
        Assert.assertEquals(jobs.get(0).getApplication().getName(), "baz-app");
        Assert.assertEquals(jobs.get(0).getApplication().getVersion(), "1.2.3");
        Assert.assertEquals(jobs.get(0).getExpectedRuntime(), new Long(500L));
    }
}