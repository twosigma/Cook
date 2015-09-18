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

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.Cancellable;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;

/**
 * Unit test for AsyncJobClient
 * <p>
 * Created: March 14, 2015
 *
 * @author wzhao
 */
@RunWith(JMockit.class)
public class JobClientTest {
    private Job _initializedJob;

    private AsyncJobClient _client;

    private JobListener _listener;

    final static ProtocolVersion _protocolVersion = new ProtocolVersion("HTTP", 1, 1);

    private static CloseableHttpResponse mockResponse(final int httpCode, final String status, final String content) {
        return new MockUp<CloseableHttpResponse>() {
            @Mock
            public StatusLine getStatusLine() {
                return new BasicStatusLine(_protocolVersion, httpCode, status);
            }

            @Mock
            public HttpEntity getEntity() {
                final BasicHttpEntity entity = new BasicHttpEntity();
                try {
                    entity.setContent(IOUtils.toInputStream(content, "UTF-8"));
                    return entity;
                } catch (IOException e) {
                    return null;
                }
            }

            @Mock(invocations = 1)
            public void close() throws IOException {}
        }.getMockInstance();
    }

    private static class MockHttpClient extends MockUp<CloseableHttpClient> {
        @Mock
        public CloseableHttpResponse execute(HttpUriRequest request)
            throws IOException
        {
            HttpRequestBase.class.cast(request).setCancellable(new MockUp<Cancellable>() {
                @Mock(invocations = 1)
                public boolean cancel() {
                    return true;
                }
            }.getMockInstance());
            // Test http post for submitting jobs.
            if (request instanceof HttpPost) {
                return mockResponse(201, "test status", "hello");
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
        AsyncJobClient.Builder builder = new AsyncJobClient.Builder();
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
