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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import mockit.Mock;
import mockit.MockUp;
import mockit.integration.junit4.JMockit;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.junit.Assert;
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

    private static String buildResponseMessage(HttpRequestBase request) {
        final Header impersonationHeader = request.getFirstHeader(JobClient.COOK_IMPERSONATE_HEADER);
        if (null != impersonationHeader) {
            return String.format("test impersonated %s", impersonationHeader.getValue());
        } else {
            return "test reason";
        }
    }

    private Job _initializedJob;
    private Job _initializedImpersonatedJob;
    private JobClient _client;
    private JobListener _listener;

    @Before
    public void setup() throws URISyntaxException {
        final Job.Builder jobBuilder = new Job.Builder();
        jobBuilder.setUUID(UUID.randomUUID());
        jobBuilder.setCommand("sleep 10s");
        jobBuilder.setMemory(100.0);
        jobBuilder.setCpus(1.0);
        jobBuilder.setApplication(new Application("sleep-app", "latest"));
        jobBuilder.setExpectedRuntime(10000L);
        _initializedJob = jobBuilder.build();

        jobBuilder.setUUID(UUID.randomUUID());
        _initializedImpersonatedJob = jobBuilder.build();

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
        _client = builder.setHost("127.0.0.1").setPort(80).setEndpoint("cook").setSubmitRetryInterval(1).build();
    }

    @Test
    public void testJobSubmit() throws JobClientException {
        // arrange
        final AtomicInteger postCounter = new AtomicInteger(0);
        new MockUp<JobClient>() {
            @Mock
            public HttpResponse executeWithRetries(HttpRequestBase request, int ignore1, long ignore2) throws IOException {
                final ProtocolVersion protocolVersion = new ProtocolVersion("HTTP", 1, 1);
                final BasicHttpEntity httpEntity = new BasicHttpEntity();
                httpEntity.setContent(IOUtils.toInputStream("hello", "UTF-8"));
                final String msg = buildResponseMessage(request);
                // Test http post for submitting jobs.
                if (request instanceof HttpPost) {
                    postCounter.incrementAndGet();
                    final BasicStatusLine statusLine = new BasicStatusLine(protocolVersion, 201, msg);
                    final BasicHttpResponse response = new BasicHttpResponse(statusLine);
                    response.setEntity(httpEntity);
                    return response;
                } else {
                    return null;
                }
            }
        };
        // act
        _client.submit(Lists.newArrayList(_initializedJob), _listener);
        // assert
        Assert.assertEquals(1, postCounter.get());
    }

    private HttpResponse executeAndReturnTransactionTimedOutError(HttpRequestBase request, AtomicInteger postCounter) throws IOException {
        final ProtocolVersion protocolVersion = new ProtocolVersion("HTTP", 1, 1);
        final BasicHttpEntity httpEntity = new BasicHttpEntity();
        final String msg = buildResponseMessage(request);
        // Test http post for submitting jobs.
        if (request instanceof HttpPost) {
            postCounter.incrementAndGet();
            final BasicStatusLine statusLine = new BasicStatusLine(protocolVersion, 500, msg);
            final BasicHttpResponse response = new BasicHttpResponse(statusLine);
            httpEntity.setContent(IOUtils.toInputStream("Transaction timed out.", "UTF-8"));
            response.setEntity(httpEntity);
            return response;
        } else {
            return null;
        }
    }

    @Test(expected = JobClientException.class)
    public void testJobSubmitWithTransactionTimeoutAndFailingEmptyQuery() throws JobClientException {
        // arrange
        final AtomicInteger postCounter = new AtomicInteger(0);
        final AtomicInteger queryCounter = new AtomicInteger(0);
        new MockUp<JobClient>() {
            @Mock
            public HttpResponse executeWithRetries(HttpRequestBase request, int ignore1, long ignore2) throws IOException {
                return executeAndReturnTransactionTimedOutError(request, postCounter);
            }

            @Mock
            public Map<UUID, Job> queryJobs(Collection<UUID> uuids) {
                queryCounter.incrementAndGet();
                Assert.assertEquals(1, uuids.size());
                return new HashMap<>();
            }
        };
        // act
        _client.submit(Lists.newArrayList(_initializedJob), _listener);
        // assert
        Assert.assertEquals(1, postCounter.get());
        Assert.assertEquals(1, queryCounter.get());
    }

    @Test(expected = JobClientException.class)
    public void testJobSubmitWithTransactionTimeoutAndFailingInQuery() throws JobClientException {
        // arrange
        final AtomicInteger postCounter = new AtomicInteger(0);
        final AtomicInteger queryCounter = new AtomicInteger(0);
        new MockUp<JobClient>() {
            @Mock
            public HttpResponse executeWithRetries(HttpRequestBase request, int ignore1, long ignore2) throws IOException {
                return executeAndReturnTransactionTimedOutError(request, postCounter);
            }

            @Mock
            public Map<UUID, Job> queryJobs(Collection<UUID> uuids) throws JobClientException {
                queryCounter.incrementAndGet();
                Assert.assertEquals(1, uuids.size());
                throw new JobClientException("Exception thrown from test");
            }
        };
        try {
            // act
            _client.submit(Lists.newArrayList(_initializedJob), _listener);
        } finally {
            // assert
            Assert.assertEquals(1, postCounter.get());
            Assert.assertEquals(1, queryCounter.get());
        }
    }

    @Test
    public void testJobSubmitWithTransactionTimeoutAndSucceeding() throws JobClientException {
        // arrange
        final AtomicInteger postCounter = new AtomicInteger(0);
        final AtomicInteger queryCounter = new AtomicInteger(0);
        new MockUp<JobClient>() {
            @Mock
            public HttpResponse executeWithRetries(HttpRequestBase request, int ignore1, long ignore2) throws IOException {
                return executeAndReturnTransactionTimedOutError(request, postCounter);
            }

            @Mock
            public Map<UUID, Job> queryJobs(Collection<UUID> uuids) {
                queryCounter.incrementAndGet();
                Assert.assertEquals(1, uuids.size());
                Map<UUID, Job> uuidToJob = new HashMap<UUID, Job>();
                uuidToJob.put(_initializedJob.getUUID(), _initializedJob);
                return uuidToJob;
            }
        };
        // act
        _client.submit(Lists.newArrayList(_initializedJob), _listener);
        // assert
        Assert.assertEquals(1, postCounter.get());
        Assert.assertEquals(1, queryCounter.get());
    }

    @Test
    public void testImpersonation() throws JobClientException {
        // arrange
        final AtomicInteger deleteCounter = new AtomicInteger(0);
        final AtomicInteger postCounter = new AtomicInteger(0);
        new MockUp<JobClient>() {
            @Mock
            public HttpResponse executeWithRetries(HttpRequestBase request, int ignore1, long ignore2) throws IOException {
                final ProtocolVersion protocolVersion = new ProtocolVersion("HTTP", 1, 1);
                final BasicHttpEntity httpEntity = new BasicHttpEntity();
                httpEntity.setContent(IOUtils.toInputStream("hello", "UTF-8"));
                final String msg = buildResponseMessage(request);
                // Test http post for submitting jobs.
                if (request instanceof HttpPost) {
                    postCounter.incrementAndGet();
                    final BasicStatusLine statusLine = new BasicStatusLine(protocolVersion, 201, msg);
                    final BasicHttpResponse response = new BasicHttpResponse(statusLine);
                    response.setEntity(httpEntity);
                    return response;
                } else if (request instanceof HttpDelete) {
                    deleteCounter.incrementAndGet();
                    final BasicStatusLine statusLine = new BasicStatusLine(protocolVersion, 204, msg);
                    final BasicHttpResponse response = new BasicHttpResponse(statusLine);
                    response.setEntity(httpEntity);
                    return response;
                } else {
                    return null;
                }
            }
        };
        // act
        _client.impersonating("foo").submit(Lists.newArrayList(_initializedImpersonatedJob), _listener);
        _client.impersonating("foo").abort(Lists.newArrayList(_initializedImpersonatedJob.getUUID()));
        // assert
        Assert.assertEquals(1, deleteCounter.get());
        Assert.assertEquals(1, postCounter.get());
    }
}
