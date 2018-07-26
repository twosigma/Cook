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

    @Test
    /** Note that this test is nondeterministic. The chances of spurious failure (right now) are < 1-in-a-million. */
    public void testJobClient() {
        long minHi = Long.MAX_VALUE;
        long maxHi = Long.MIN_VALUE;
        long minLo = Long.MAX_VALUE;
        long maxLo = Long.MIN_VALUE;
        for (int ii = 0 ; ii < 200 ; ii++) {
            UUID uuid = JobClient.makeTemporalUUID();

            maxHi = Math.max(maxHi,uuid.getMostSignificantBits());
            minHi = Math.min(minHi,uuid.getMostSignificantBits());
            maxLo = Math.max(maxLo,uuid.getLeastSignificantBits());
            minLo = Math.min(minLo,uuid.getLeastSignificantBits());
        }

        // If we pick 1000 random UUID's, they should have certain patterns between min and max.
        //    * The temporal bits should be clustered into a small bit of the bitspace.
        //    * The random bit max and min should be widely separated.
        //    * The random bit max and min should be close, but not too close, to aspymototes

        // High bits are a count seconds since epoch, so we expect there to be at least
        // NOTE: Will be flaky test if generation takes >5 second, which would be weird.
        // However, travis can be slow. If so, try increasing this.

        // Temporal bits (the high 32 count seconds) should tightly clustered;
        // This assumes generation takes at most 5 seconds.
        Assert.assertTrue(maxHi-minHi < (5L<<32));

        // And we should have some difference between min and max; Most of lowest 24 bits are random after all.
        Assert.assertTrue(maxHi-minHi > (1L<<16));

        // Low order bits in UUID are always negative, due to a type flag encoded in UUID's.
        // Negate this to make it nicer to manipulate as java's signed longs.
        minLo = -minLo;
        maxLo = -maxLo;

        // Premise of these checks is that the minimum should be 'close' to the smallest it can be, but not too close.
        // Expect this to be close to 1<<63, less than 1<<63, but not too close.
        Assert.assertTrue( minLo < (1L << 63) - (1L<<32)); // Not too close, not closer than ~1/1E9.
        Assert.assertTrue( minLo > (1L << 63) - (1L<<60)); // Close, within 8% of end.

        // Expect this to be clsoe to 1<<62, and greater than 1<<62, but not too close.
        Assert.assertTrue( maxLo > (1L<<62)+(1L << 32)); // Not too close
        Assert.assertTrue( maxLo < (1L<<62)+(1L << 59)); // Close

        // If these pass, we've hopefully got at least 62 bits of randomness.
    }
}
