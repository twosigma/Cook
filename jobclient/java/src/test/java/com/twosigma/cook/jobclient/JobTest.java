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
import com.twosigma.cook.jobclient.constraint.Operator;
import com.twosigma.cook.jobclient.Checkpoint.CheckpointOptions;
import com.twosigma.cook.jobclient.Checkpoint.Mode;
import com.twosigma.cook.jobclient.Checkpoint.PeriodicCheckpointOptions;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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
    private static final double EPSILON = 1e-15;

    private class TestInstanceDecorator implements InstanceDecorator {
        @Override
        public Instance.Builder decorate(Instance.Builder builder) {
            builder.setExecutorID("test-123");
            return builder;
        }
    }

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
        jobBuilder.setGpus(2);
        jobBuilder.addEnv("FOO", "test");
        jobBuilder.addGpuModelEnv("nvidia-tesla-p100");
        jobBuilder.addLabel("foobar", "frobnicator");
        jobBuilder.setMaxRuntime(1000L);
        jobBuilder.disableMeaCulpaRetries();
        jobBuilder.addUri(new FetchableURI.Builder().setValue("http://example.com/my_resource").build());
        jobBuilder.setApplication(new Application("baz-app", "1.2.3", "workloadClass", "workloadId", "workloadDetails"));
        jobBuilder.setCheckpoint(new Checkpoint(Mode.auto,
                new CheckpointOptions(new HashSet<String>(Arrays.asList("path1", "path2", "path2"))),
                new PeriodicCheckpointOptions(5)));
        jobBuilder.setExpectedRuntime(500L);
        jobBuilder.addConstraint(_constraint1);
        jobBuilder.addConstraint(Collections.singletonList(_constraint2));
        jobBuilder.addNodeTypeConstraint("c2-standard-16");
        jobBuilder.addNodeFamilyConstraint("c2");
        jobBuilder.addCpuArchitectureConstraint("intel-cascade-lake");
    }

    @Test
    public void testJsonizeJobBasic() throws JSONException {
        final Job.Builder jobBuilder = new Job.Builder();
        populateBuilder(jobBuilder);
        final Job basicJob = jobBuilder.build();

        final JSONObject jsonJob = Job.jsonizeJob(basicJob);
        Assert.assertEquals(jsonJob.getInt("gpus"), basicJob.getGpus().intValue());
        Assert.assertEquals(jsonJob.getString("uuid"), basicJob.getUUID().toString());
        Assert.assertEquals(
                new JSONObject()
                        .put("name", "baz-app")
                        .put("version", "1.2.3")
                        .put("workload-class", "workloadClass")
                        .put("workload-id", "workloadId")
                        .put("workload-details", "workloadDetails")
                        .toString(),
                jsonJob.getJSONObject("application").toString());
        Assert.assertEquals(jsonJob.getJSONObject("checkpoint").getString("mode"),
                basicJob.getCheckpoint().getMode().toString());
        Assert.assertEquals(500L, jsonJob.getLong("expected_runtime"));
        Assert.assertEquals(true, jsonJob.getBoolean("disable_mea_culpa_retries"));
        JSONArray constraints = jsonJob.getJSONArray("constraints");
        Assert.assertEquals(constraints.length(), 5);
        Assert.assertEquals(_constraint1.toJson().toString(), constraints.getJSONArray(0).toString());
        Assert.assertEquals(_constraint2.toJson().toString(), constraints.getJSONArray(1).toString());
        Assert.assertEquals(
                new JSONArray(new String[]{"node-type", "EQUALS", "c2-standard-16"}).toString(),
                constraints.getJSONArray(2).toString());
        Assert.assertEquals(
                new JSONArray(new String[]{"node-family", "EQUALS", "c2"}).toString(),
                constraints.getJSONArray(3).toString());
        Assert.assertEquals(
                new JSONArray(new String[]{"cpu-architecture", "EQUALS", "intel-cascade-lake"}).toString(),
                constraints.getJSONArray(4).toString());
        Assert.assertEquals(
                jsonJob.getJSONObject("env").toString(),
                new JSONObject().put("COOK_GPU_MODEL", "nvidia-tesla-p100").put("FOO", "test").toString());
    }

    @Test
    public void testParseFromJsonBasic() throws JSONException {
        final Job.Builder jobBuilder = new Job.Builder();
        populateBuilder(jobBuilder);
        final Job basicJob = jobBuilder.build();

        final JSONObject json = Job.jsonizeJob(basicJob);
        json.put("pool", "dummy-pool");
        final String jsonString = new JSONArray().put(json).toString();
        final List<Job> jobs = Job.parseFromJSON(jsonString);
        Assert.assertEquals(jobs.size(), 1);
        final Job job = jobs.get(0);
        Assert.assertEquals(basicJob, job);
        Assert.assertEquals(basicJob.getExecutor(), job.getExecutor());
        Assert.assertEquals(job.getMaxRuntime(), Long.valueOf(1000L));
        Assert.assertEquals(job.getApplication().getName(), "baz-app");
        Assert.assertEquals(job.getApplication().getVersion(), "1.2.3");
        Assert.assertEquals(job.getApplication().getWorkloadClass(), "workloadClass");
        Assert.assertEquals(job.getApplication().getWorkloadId(), "workloadId");
        Assert.assertEquals(job.getApplication().getWorkloadDetails(), "workloadDetails");
        Assert.assertEquals(job.getCheckpoint().getMode(), Mode.auto);
        Assert.assertEquals(job.getCheckpoint().getCheckpointOptions().getPreservePaths(),
                new HashSet<String>(Arrays.asList("path1", "path2")));
        Assert.assertEquals(job.getCheckpoint().getPeriodicCheckpointOptions().getPeriodSec(), 5);
        Assert.assertEquals(job.getExpectedRuntime(), Long.valueOf(500L));
        Assert.assertEquals(job.getPool(), "dummy-pool");
        Assert.assertEquals(Integer.valueOf(2), job.getGpus());
        Assert.assertEquals("nvidia-tesla-p100", job.getEnv().get("COOK_GPU_MODEL"));
        Assert.assertEquals("test", job.getEnv().get("FOO"));
        Assert.assertNull(job.getDisk().getRequest());
        Assert.assertNull(job.getDisk().getLimit());
        Assert.assertNull(job.getDisk().getType());

        final Set<Constraint> constraints = job.getConstraints();
        Assert.assertEquals(constraints.size(), 5);
        Iterator<Constraint> iter = constraints.iterator();
        Assert.assertEquals(_constraint1, iter.next());
        Assert.assertEquals(_constraint2, iter.next());
        Constraint constraint = iter.next();
        Assert.assertEquals("node-type", constraint.getAttribute());
        Assert.assertEquals(Operator.EQUALS, constraint.getOperator());
        constraint = iter.next();
        Assert.assertEquals("node-family", constraint.getAttribute());
        Assert.assertEquals(Operator.EQUALS, constraint.getOperator());
        constraint = iter.next();
        Assert.assertEquals("cpu-architecture", constraint.getAttribute());
        Assert.assertEquals(Operator.EQUALS, constraint.getOperator());
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

            final JSONObject json = Job.jsonizeJob(jobBuilder.build());
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

        final JSONObject json = Job.jsonizeJob(jobBuilder.build());
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

        final JSONObject json = Job.jsonizeJob(jobBuilder.build());
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

        final JSONObject json = Job.jsonizeJob(jobBuilder.build());
        final String jsonString = new JSONArray().put(json).toString();
        final List<Job> jobs = Job.parseFromJSON(jsonString);
        Assert.assertEquals(jobs.size(), 1);
        final Job actualJob = jobs.get(0);

        Assert.assertEquals(datasets.toString(), actualJob.getDatasets().toString());
    }

    @Test
    public void testParseFromJsonNoGpus() {
        final String jsonString = "[{" +
                "\"uuid\": \"" + UUID.randomUUID() + "\", " +
                "\"mem\": 1, " +
                "\"cpus\": 0.1, " +
                "\"command\": \"\", " +
                "\"priority\": 1, " +
                "\"status\": \"waiting\", " +
                "\"max_retries\": 1, " +
                "\"max_runtime\": 1, " +
                "\"instances\": []" +
                "}]";
        final List<Job> jobs = Job.parseFromJSON(jsonString, null);
        final Job actualJob = jobs.get(0);
        Assert.assertEquals(0, (long) actualJob.getGpus());
    }

    @Test
    public void testJsonizeJobWithDisk() {
        final Job.Builder jobBuilder = new Job.Builder();
        populateBuilder(jobBuilder);
        jobBuilder.setDiskRequest(10.0);
        jobBuilder.setDiskLimit(20.0);
        jobBuilder.setDiskType("standard");

        final Job basicJob = jobBuilder.build();
        final JSONObject jsonJob = Job.jsonizeJob(basicJob);

        Assert.assertEquals(10.0, basicJob.getDisk().getRequest(), EPSILON);
        Assert.assertEquals(20.0, basicJob.getDisk().getLimit(), EPSILON);
        Assert.assertEquals("standard", basicJob.getDisk().getType());

        Assert.assertEquals(10.0, jsonJob.getJSONObject("disk").getDouble("request"), EPSILON);
        Assert.assertEquals(20.0, jsonJob.getJSONObject("disk").getDouble("limit"), EPSILON);
        Assert.assertEquals("standard", jsonJob.getJSONObject("disk").getString("type"));
    }

    @Test
    public void testParseFromJsonDisk() {
        final Job.Builder jobBuilder1 = new Job.Builder();
        populateBuilder(jobBuilder1);
        jobBuilder1.setDiskRequest(10.0);
        jobBuilder1.setDiskType("pd-ssd");

        final JSONObject json = Job.jsonizeJob(jobBuilder1.build());
        final String jsonString = new JSONArray().put(json).toString();
        final List<Job> jobs = Job.parseFromJSON(jsonString);
        Assert.assertEquals(jobs.size(), 1);
        final Job actualJob1 = jobs.get(0);

        Assert.assertEquals(10.0, actualJob1.getDisk().getRequest(), EPSILON);
        Assert.assertNull(actualJob1.getDisk().getLimit());
        Assert.assertEquals("pd-ssd", actualJob1.getDisk().getType());

        final Disk diskEntry = new Disk(50.0, 100.0, "standard");
        final Job.Builder jobBuilder2 = new Job.Builder();
        populateBuilder(jobBuilder2);
        jobBuilder2.setDisk(diskEntry);

        final JSONObject json2 = Job.jsonizeJob(jobBuilder2.build());
        final String jsonString2 = new JSONArray().put(json2).toString();
        final List<Job> jobs2 = Job.parseFromJSON(jsonString2);
        Assert.assertEquals(jobs2.size(), 1);
        final Job actualJob2 = jobs2.get(0);

        Assert.assertEquals(diskEntry.getRequest(), actualJob2.getDisk().getRequest(), EPSILON);
        Assert.assertEquals(diskEntry.getLimit(), actualJob2.getDisk().getLimit(), EPSILON);
        Assert.assertEquals(diskEntry.getType(), actualJob2.getDisk().getType());
    }

    @Test
    public void testParseFromJsonToBuilderOfJob() {
        final Job.Builder jobBuilder1 = new Job.Builder();
        populateBuilder(jobBuilder1);
        jobBuilder1.setDiskRequest(10.0);
        jobBuilder1.setDiskType("pd-ssd");

        final JSONObject json = Job.jsonizeJob(jobBuilder1.build());
        final Job job1 = Job.parseFromJSON(json);

        final Job.Builder jobBuilder2 = new Job.Builder().of(job1);
        final Job job2 = jobBuilder2.build();

        Assert.assertEquals(job1.getCommand(), job2.getCommand());
        Assert.assertEquals(job1.getEnv(), job2.getEnv());
        Assert.assertEquals(job1.getDisk().getRequest(), job2.getDisk().getRequest(), EPSILON);
        Assert.assertEquals(job1.getDisk().getType(), job2.getDisk().getType());
        Assert.assertEquals(job1.getConstraints().size(), job2.getConstraints().size());
    }

    @Test
    public void testParseFromJsonToBuilder() {
        final Job.Builder jobBuilder1 = new Job.Builder();
        populateBuilder(jobBuilder1);
        jobBuilder1.setDiskRequest(10.0);
        jobBuilder1.setDiskType("pd-ssd");
        final Job job1 = jobBuilder1.build();

        final JSONObject json = Job.jsonizeJob(jobBuilder1.build());
        final Job.Builder jobBuilder2 = new Job.Builder().parseFromJSON(json);
        jobBuilder2.addEnv("foo", "bar");
        jobBuilder2.addLabel("foo", "bar");
        final Job job2 = jobBuilder2.build();

        Assert.assertEquals(job1.getCpus(), job2.getCpus());
        Assert.assertEquals(job1.getMemory(), job2.getMemory());
        Assert.assertEquals(job1.getDisk().getRequest(), job2.getDisk().getRequest(), EPSILON);
        Assert.assertEquals(job1.getDisk().getType(), job2.getDisk().getType());
        Assert.assertEquals(job1.getConstraints().size(), job2.getConstraints().size());
        // Added an extra label to job1.
        Assert.assertEquals(job2.getLabels().size(), job1.getLabels().size() + 1);
    }


    @Test
    public void testParseFromJSONWithDecorator() {
        final Job.Builder jobBuilder1 = new Job.Builder();
        populateBuilder(jobBuilder1);

        final JSONObject instanceJson = new JSONObject();
        instanceJson.put("slave_id", "20150311-033720-1963923116-5050-4084-34");
        instanceJson.put("start_time", 1426632249597L);
        instanceJson.put("end_time", 1426632249597L);
        instanceJson.put("status", Instance.Status.SUCCESS);
        instanceJson.put("preempted", false);
        instanceJson.put("hostname", "foo.bar.com");
        instanceJson.put("task_id", UUID.randomUUID());
        instanceJson.put("executor_id", "f52fbacf-52a1-44a2-bda1-cbfa477cc164");

        final JSONObject json = Job.jsonizeJob(jobBuilder1.build());
        json.put("instances", new JSONArray().put(instanceJson));
        final String jsonString = new JSONArray().put(json).toString();

        // Decorator will set executor ID to test-123
        final InstanceDecorator dec = new TestInstanceDecorator();
        final List<Job> jobs = Job.parseFromJSON(jsonString, dec);

        Assert.assertEquals(jobs.size(), 1);
        final Job actualJob = jobs.get(0);
        final Instance instance = actualJob.getInstances().get(0);
        Assert.assertEquals(instance.getExecutorID(), "test-123");
    }
}
