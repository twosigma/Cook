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
 * code.
 */

package com.twosigma.cook.jobclient;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.UUID;

import mockit.Mock;
import mockit.MockUp;
import mockit.integration.junit4.JMockit;

import org.apache.http.client.HttpClient;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;

/**
 * Unit test for JobClient
 * <p>
 * Created: March 14, 2015
 *
 * @author wzhao
 */
@RunWith(JMockit.class)
public class JobClientTest {
    private Job _initializedJob;

    private JobClient _client;

    private JobListener _listener;

    private static class MockHttpClient extends MockUp<JobClient> {
        @Mock
        public HttpResponse executeWithRetries(HttpRequestBase request, int ignore1, long ignore2) throws IOException {
            final ProtocolVersion protocolVersion = new ProtocolVersion("HTTP", 1, 1);
            final BasicHttpEntity httpEntity = new BasicHttpEntity();
            httpEntity.setContent(IOUtils.toInputStream("hello", "UTF-8"));
            // Test http post for submitting jobs.
            if (request instanceof HttpPost) {
                final BasicStatusLine statusLine =
                        new BasicStatusLine(protocolVersion, 201, "test reason");
                final BasicHttpResponse response = new BasicHttpResponse(statusLine);
                response.setEntity(httpEntity);
                return response;
            } else {
                return null;
            }
        }
    }

    @Before
    public void setup() throws URISyntaxException {
        new MockHttpClient();
        final Job.Builder jobBuilder = new Job.Builder();
        jobBuilder.setUUID(UUID.randomUUID());
        jobBuilder.setCommand("sleep 10s");
        jobBuilder.setMemory(100.0);
        jobBuilder.setCpus(1.0);
        _initializedJob = jobBuilder.build();

        // Create the job client.
        JobClient.Builder builder = new JobClient.Builder();
        _listener = new JobListener() {
            @Override
            public void onStatusUpdate(Job job) {
                String log = "Job changed to " + job;
                System.out.println(log);
                if (job.getStatus() == Job.Status.COMPLETED) {
                    System.out.println("Finally, is job success?: " + job.isSuccess());
                }
            }
        };
        _client = builder.setHost("127.0.0.1").setPort(80).setEndpoint("cook").build();

    }

    @Test
    public void test() throws JobClientException {
        _client.submit(Lists.newArrayList(_initializedJob), _listener);
    }
}
