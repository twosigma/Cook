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

import com.twosigma.cook.jobclient.constraint.Constraint;
import com.twosigma.cook.jobclient.constraint.Constraints;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Unit tests for {@link Job}.
 *
 * @author wzhao
 */
public class JobTest {

    // Constraints which could be used for any test.
    private Constraint _constraint1;
    private Constraint _constraint2;

    @Before
    public void setup() {
        _constraint1 = Constraints.buildEqualsConstraint("bar1", "foo1");
        _constraint2 = Constraints.buildEqualsConstraint("bar2", "foo2");
    }

    private void populateBuilder(Job.Builder jobBuilder) {
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
        jobBuilder.addConstraint(_constraint1);
        jobBuilder.addConstraint(Collections.singletonList(_constraint2));
    }

    private JSONObject convertJobToJsonObject(Job basicJob) {
        final JSONObject json = Job.jsonizeJob(basicJob);
        if (!json.has("instances")) {
            json.put("instances", new JSONArray());
        }
        if (!json.has("status")) {
            json.put("status", "INITIALIZED");
        }
        return json;
    }

    @Test
    public void testJsonizeJobBasic() throws JSONException {
        final Job.Builder jobBuilder = new Job.Builder();
        populateBuilder(jobBuilder);
        final Job basicJob = jobBuilder.build();

        final JSONObject jsonJob = Job.jsonizeJob(basicJob);
        Assert.assertEquals(jsonJob.getString("uuid"), basicJob.getUUID().toString());
        Assert.assertEquals(
            jsonJob.getJSONObject("application").toString(),
            new JSONObject().put("name", "baz-app").put("version", "1.2.3").toString());
        Assert.assertEquals(500L, jsonJob.getLong("expected_runtime"));
        Assert.assertEquals(true, jsonJob.getBoolean("disable_mea_culpa_retries"));
        JSONArray constraints = jsonJob.getJSONArray("constraints");
        Assert.assertEquals(constraints.length(), 2);
        Assert.assertEquals(constraints.getJSONArray(0).toString(), _constraint1.toJson().toString());
        Assert.assertEquals(constraints.getJSONArray(1).toString(), _constraint2.toJson().toString());
    }

    @Test
    public void testParseFromJsonBasic() throws JSONException {
        final Job.Builder jobBuilder = new Job.Builder();
        populateBuilder(jobBuilder);
        final Job basicJob = jobBuilder.build();

        final JSONObject json = convertJobToJsonObject(basicJob);
        json.put("pool", "dummy-pool");
        final String jsonString = new JSONArray().put(json).toString();
        final List<Job> jobs = Job.parseFromJSON(jsonString);
        Assert.assertEquals(jobs.size(), 1);
        final Job job = jobs.get(0);
        Assert.assertEquals(basicJob, job);
        Assert.assertEquals(basicJob.getExecutor(), job.getExecutor());
        Assert.assertEquals(job.getMaxRuntime(), new Long(1000L));
        Assert.assertEquals(job.getApplication().getName(), "baz-app");
        Assert.assertEquals(job.getApplication().getVersion(), "1.2.3");
        Assert.assertEquals(job.getExpectedRuntime(), new Long(500L));
        Assert.assertEquals(job.getPool(), "dummy-pool");

        final Set<Constraint> constraints = job.getConstraints();
        Assert.assertEquals(constraints.size(), 2);
        Iterator<Constraint> iter = constraints.iterator();
        Constraint constraint = iter.next();
        Assert.assertEquals(constraint, _constraint1);
        constraint = iter.next();
        Assert.assertEquals(constraint, _constraint2);
    }

    @Test
    public void testJsonizeJobExecutor() throws JSONException {
        for (Executor executor : Executor.values()) {
            final Job.Builder jobBuilder = new Job.Builder();
            populateBuilder(jobBuilder);
            jobBuilder.setExecutor(executor.displayName());

            final Job basicJob = jobBuilder.build();
            final JSONObject jsonJob = Job.jsonizeJob(basicJob);

            Assert.assertEquals(executor.displayName(), basicJob.getExecutor().displayName());
            Assert.assertEquals(executor.displayName(), jsonJob.getString("executor"));
        }
    }

    @Test
    public void testParseFromJsonExecutor() throws JSONException {
        for (Executor executor : Executor.values()) {
            final Job.Builder jobBuilder = new Job.Builder();
            populateBuilder(jobBuilder);
            jobBuilder.setExecutor(executor.displayName());

            final JSONObject json = convertJobToJsonObject(jobBuilder.build());
            final String jsonString = new JSONArray().put(json).toString();
            final List<Job> jobs = Job.parseFromJSON(jsonString);
            Assert.assertEquals(jobs.size(), 1);
            final Job actualJob = jobs.get(0);

            Assert.assertEquals(executor, actualJob.getExecutor());
        }
    }

    @Test
    public void testJsonizeJobProgressOutputFile() throws JSONException {
        final String progressOutputFile = "/path/to/progress/output";

        final Job.Builder jobBuilder = new Job.Builder();
        populateBuilder(jobBuilder);
        jobBuilder.setProgressOutputFile(progressOutputFile);

        final Job basicJob = jobBuilder.build();
        final JSONObject jsonJob = Job.jsonizeJob(basicJob);

        Assert.assertEquals(progressOutputFile, basicJob.getProgressOutputFile());
        Assert.assertEquals(progressOutputFile, jsonJob.getString("progress_output_file"));
    }

    @Test
    public void testParseFromJsonProgressOutputFile() throws JSONException {
        final String progressOutputFile = "/path/to/progress/output";

        final Job.Builder jobBuilder = new Job.Builder();
        populateBuilder(jobBuilder);
        jobBuilder.setProgressOutputFile(progressOutputFile);

        final JSONObject json = convertJobToJsonObject(jobBuilder.build());
        final String jsonString = new JSONArray().put(json).toString();
        final List<Job> jobs = Job.parseFromJSON(jsonString);
        Assert.assertEquals(jobs.size(), 1);
        final Job actualJob = jobs.get(0);

        Assert.assertEquals(progressOutputFile, actualJob.getProgressOutputFile());
    }

    @Test
    public void testJsonizeJobProgressRegexString() throws JSONException {
        final String progressOutputFile = "/path/to/progress/output";

        final Job.Builder jobBuilder = new Job.Builder();
        populateBuilder(jobBuilder);
        jobBuilder.setProgressRegexString(progressOutputFile);

        final Job basicJob = jobBuilder.build();
        final JSONObject jsonJob = Job.jsonizeJob(basicJob);

        Assert.assertEquals(progressOutputFile, basicJob.getProgressRegexString());
        Assert.assertEquals(progressOutputFile, jsonJob.getString("progress_regex_string"));
    }

    @Test
    public void testParseFromJsonProgressRegexString() throws JSONException {
        final String progressOutputFile = "SomeProgressRegex";

        final Job.Builder jobBuilder = new Job.Builder();
        populateBuilder(jobBuilder);
        jobBuilder.setProgressRegexString(progressOutputFile);

        final JSONObject json = convertJobToJsonObject(jobBuilder.build());
        final String jsonString = new JSONArray().put(json).toString();
        final List<Job> jobs = Job.parseFromJSON(jsonString);
        Assert.assertEquals(jobs.size(), 1);
        final Job actualJob = jobs.get(0);

        Assert.assertEquals(progressOutputFile, actualJob.getProgressRegexString());
    }

    @Test
    public void testJsonizeDatasets() {
        final JSONObject dataset = new JSONObject();
        dataset.put("foo", "bar");
        final JSONArray datasets = new JSONArray();
        datasets.put(dataset);

        final Job.Builder jobBuilder = new Job.Builder();
        populateBuilder(jobBuilder);
        jobBuilder.setDatasets(datasets);
        final Job basicJob = jobBuilder.build();
        final JSONObject jsonJob = Job.jsonizeJob(basicJob);

        Assert.assertEquals(datasets.toString(), basicJob.getDatasets().toString());
        Assert.assertEquals(datasets.toString(), jsonJob.getJSONArray("datasets").toString());
    }

    @Test
    public void testParseDatasets() {
        final JSONObject dataset = new JSONObject();
        dataset.put("foo", "bar");
        final JSONArray datasets = new JSONArray();
        datasets.put(dataset);

        final Job.Builder jobBuilder = new Job.Builder();
        populateBuilder(jobBuilder);
        jobBuilder.setDatasets(datasets);

        final JSONObject json = convertJobToJsonObject(jobBuilder.build());
        final String jsonString = new JSONArray().put(json).toString();
        final List<Job> jobs = Job.parseFromJSON(jsonString);
        Assert.assertEquals(jobs.size(), 1);
        final Job actualJob = jobs.get(0);

        Assert.assertEquals(datasets.toString(), actualJob.getDatasets().toString());
    }
}
