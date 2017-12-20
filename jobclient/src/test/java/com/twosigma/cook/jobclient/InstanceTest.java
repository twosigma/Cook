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

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

/**
 * Unit tests for {@link Job}.
 *
 * @author wzhao
 */
public class InstanceTest {
    private void populateBuilder(Instance.Builder instanceBuilder) {
        instanceBuilder.setTaskID(UUID.randomUUID());
        instanceBuilder.setSlaveID("20150311-033720-1963923116-5050-4084-32");
        instanceBuilder.setStartTime(1426632249597L);
        instanceBuilder.setEndTime(1426632251828L);
        instanceBuilder.setHostName("server1.example.com");
        instanceBuilder.setExecutorID("f52fbacf-52a1-44a2-bda1-cbfa477cc163");
        instanceBuilder.setStatus(Instance.Status.SUCCESS);
        instanceBuilder.setPreempted(false);
    }

    private void populateJson(JSONObject json, Instance _successfulInstance) {
        json.put("slave_id", _successfulInstance.getSlaveID());
        json.put("start_time", _successfulInstance.getStartTime());
        json.put("end_time", _successfulInstance.getEndTime());
        json.put("status", _successfulInstance.getStatus());
        json.put("preempted", _successfulInstance.getPreempted());
        json.put("hostname", _successfulInstance.getHostName());
        json.put("task_id", _successfulInstance.getTaskID());
        json.put("executor_id", _successfulInstance.getExecutorID());
    }

    @Test
    public void testBuilderBasic() throws JSONException {
        final Instance.Builder instanceBuilder = new Instance.Builder();
        populateBuilder(instanceBuilder);

        Instance basicInstance = instanceBuilder.build();

        Assert.assertNull(basicInstance.getExecutor());
        Assert.assertEquals(Instance.Status.SUCCESS, basicInstance.getStatus());
        Assert.assertNotNull(basicInstance.getTaskID());
    }

    @Test
    public void testParseFromJsonBasic() throws JSONException {
        final Instance.Builder instanceBuilder = new Instance.Builder();
        populateBuilder(instanceBuilder);
        Instance basicInstance = instanceBuilder.build();

        final JSONObject json = new JSONObject();
        populateJson(json, basicInstance);

        final String jsonString = new JSONArray().put(json).toString();
        final List<Instance> instances = Instance.parseFromJSON(jsonString);

        Assert.assertEquals(instances.size(), 1);
        Assert.assertEquals(instances.get(0), basicInstance);
    }

    @Test
    public void testBuilderWithExecutor() throws JSONException {
        for (Executor executor : Executor.values()) {
            final Instance.Builder instanceBuilder = new Instance.Builder();
            populateBuilder(instanceBuilder);
            instanceBuilder.setExecutor(executor.displayName());

            Instance basicInstance = instanceBuilder.build();

            Assert.assertEquals(executor, basicInstance.getExecutor());
        }
    }

    @Test
    public void testParseFromJsonWithExecutor() throws JSONException {

        final Instance.Builder instanceBuilder = new Instance.Builder();
        populateBuilder(instanceBuilder);
        Instance basicInstance = instanceBuilder.build();

        Assert.assertNull(basicInstance.getExecutor());

        for (Executor executor : Executor.values()) {
            final JSONObject json = new JSONObject();
            populateJson(json, basicInstance);
            json.put("executor", executor.displayName());

            final String jsonString = new JSONArray().put(json).toString();
            final List<Instance> instances = Instance.parseFromJSON(jsonString);

            Assert.assertEquals(instances.size(), 1);
            final Instance actualInstance = instances.get(0);
            Assert.assertEquals(executor, actualInstance.getExecutor());
        }
    }
}

