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

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.security.Principal;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.HttpStatus;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twosigma.cook.jobclient.auth.spnego.BasicSPNegoSchemeFactory;
import com.twosigma.cook.jobclient.auth.spnego.GSSCredentialProvider;

import org.apache.log4j.Logger;

/**
 * An implementation for the Cook job client.
 * <p>
 * This client supports the following three key operations<br>
 * -- submit: submit jobs to Cook<br>
 * -- query: query jobs status along with their instances from Cook<br>
 * -- abort: abort jobs from Cook<br>
 * <p>
 * Note that this client only tracks jobs submitted through this client. Periodically, it queries the Cook scheduler
 * endpoint for jobs status updates. If any job status changes, it will<br>
 * -- update internal map from UUID to job<br>
 * -- invoke listener call back method<br>
 * <p>
 * Also note that, each job could potentially be associated with a {@link JobListener}. However, different jobs could
 * reference the same job client.
 * <p>
 * Created: March 14, 2015
 *
 * @author wzhao
 */
public class JobClient implements Closeable {

    private static final Logger _log = Logger.getLogger(JobClient.class);

    /**
     * A builder for the {@link JobClient}.
     */
    public static class Builder
    {
        private String _host;

        private String _endpoint;

        private Integer _port;

        public static final int DEFAULT_STATUS_UPDATE_INTERVAL_SECONDS = 10;

        public static final int DEFAULT_BATCH_REQUEST_SIZE = 32;

        public static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 60;

        /**
         * An interval in seconds which will be used to query job status update periodically.
         */
        private Integer _statusUpdateIntervalSeconds;

        /**
         * The number of jobs per http request for job submission or status query.
         */
        private Integer _batchRequestSize;

        /**
         * The timeout in seconds for all http requests.
         */
        private Integer _requestTimeoutSeconds;

        private HttpClientBuilder _httpClientBuilder;

        private InstanceDecorator _instanceDecorator;

        public Builder() {
            _httpClientBuilder = HttpClientBuilder.create();
        }

        /**
         * Prior to {@code Build()}, host, port, and endpoint for the Cook scheduler must be specified either by user or
         * properties.
         *
         * @return a {@link JobClient}.
         * @throws URISyntaxException
         */
        public JobClient build() throws URISyntaxException {
            // The definition of the following parameters are optional.
            if (_statusUpdateIntervalSeconds == null) {
                _statusUpdateIntervalSeconds = DEFAULT_STATUS_UPDATE_INTERVAL_SECONDS;
            }
            if (_batchRequestSize == null) {
                _batchRequestSize = DEFAULT_BATCH_REQUEST_SIZE;
            }
            if (_requestTimeoutSeconds == null) {
                _requestTimeoutSeconds = DEFAULT_REQUEST_TIMEOUT_SECONDS;
            }
            RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(_requestTimeoutSeconds * 1000)
                .setConnectTimeout(_requestTimeoutSeconds * 1000)
                .setConnectionRequestTimeout(_requestTimeoutSeconds * 1000).setStaleConnectionCheckEnabled(true)
                .build();
            _httpClientBuilder.setDefaultRequestConfig(requestConfig);
            _httpClientBuilder.setRetryHandler(new StandardHttpRequestRetryHandler());
            return new JobClient(
                    Preconditions.checkNotNull(_host, "host must be set"),
                    Preconditions.checkNotNull(_port, "port must be set"),
                    Preconditions.checkNotNull(_endpoint, "endpoint must be set"),
                    _statusUpdateIntervalSeconds,
                    _batchRequestSize,
                    _instanceDecorator,
                    _httpClientBuilder.build());
        }

        public Builder setUsernameAuth(String username, String password) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            _httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return this;
        }

        public Builder setKerberosAuth() {
            return setKerberosAuth(null);
        }

        public Builder setKerberosAuth(GSSCredentialProvider gssCrendentialProvider) {
            Credentials creds = new Credentials() {
                @Override
                public String getPassword() {
                    return null;
                }

                @Override
                public Principal getUserPrincipal() {
                    return null;
                }
            };
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(new AuthScope(_host, _port,
                    AuthScope.ANY_REALM, AuthSchemes.SPNEGO), creds);
            _httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);

            AuthSchemeProvider authSchemaProvider = gssCrendentialProvider == null ?
                    new SPNegoSchemeFactory(true) : new BasicSPNegoSchemeFactory(true, gssCrendentialProvider);

            _httpClientBuilder.setDefaultAuthSchemeRegistry(RegistryBuilder
                                                            .<AuthSchemeProvider>create()
                                                            .register(AuthSchemes.SPNEGO, authSchemaProvider)
                                                            .build());
            return this;
        }

        /**
         * Set the Cook scheduler host where the job client expected to build will connect to.
         *
         * @param host {@link String} specifies the Cook scheduler host.
         * @return this builder.
         */
        public Builder setHost(String host) {
            Preconditions.checkNotNull(host, "host can not be null!");
            _host = host;
            return this;
        }

        public String getHost() {
            return _host;
        }

        /**
         * Set the Cook scheduler port where the job client expected to build will connect to.
         *
         * @param port specifies the Cook scheduler port.
         * @return this builder.
         */
        public Builder setPort(int port) {
            Preconditions.checkNotNull(port, "port can not be null!");
            _port = port;
            return this;
        }

        public Integer getPort() {
            return _port;
        }

        /**
         * Set the Cook scheduler endpoint where the job client expected to build will send the requests to.
         *
         * @param endpoint {@link String} specifies the Cook scheduler endpoint.
         * @return this builder.
         */
        public Builder setEndpoint(String endpoint) {
            if (!endpoint.startsWith("/")) {
                _endpoint = "/" + endpoint;
            } else {
                _endpoint = endpoint;
            }
            return this;
        }

        public String getEndpoint() {
            return _endpoint;
        }

        /**
         * Set the status update interval in seconds for the job client expected to build.
         * <p>
         * Note that as the job client only tracks jobs submitted through this client and periodically queries the Cook
         * scheduler endpoint for jobs status updates. This implies that the smaller of the interval, the more http
         * requests will be sent to the Cook scheduler endpoint. The actual status update interval for job client
         * expected to build will use the following interval:<br>
         * <code>
         * max(intervalSeconds, DEFAULT_STATUS_UPDATE_INTERVAL_SECONDS)
         * </code>
         *
         * @param intervalSeconds specifies the status update interval in seconds.
         * @return this builder.
         */
        public Builder setStatusUpdateInterval(int intervalSeconds) {
            // make sure that the interval must be at least 10 seconds.
            _statusUpdateIntervalSeconds = Math.max(intervalSeconds, DEFAULT_STATUS_UPDATE_INTERVAL_SECONDS);
            _log.info("The status update interval in seconds is " + _statusUpdateIntervalSeconds);
            return this;
        }

        public Integer getStatusUpdateInterval() {
            return _statusUpdateIntervalSeconds;
        }

        /**
         * Set the size of batch requests for the job client expected to build. This will limit the number of jobs per
         * any HTTP request through Cook scheduler rest endpoint.
         *
         * @param batchRequestSize specifies the maximum number of jobs per any http request.
         * @return this builder.
         */
        public Builder setBatchRequestSize(int batchRequestSize) {
            Preconditions.checkArgument(batchRequestSize > 0, "The batch request size must be > 0.");
            _batchRequestSize = batchRequestSize;
            return this;
        }

        public Integer getBatchRequestSize() {
            return _batchRequestSize;
        }

        /**
         * Set HTTP request timeout in seconds expected to set SocketTimeout, ConnectionTimeout, and
         * ConnectionRequestTimeout for the HTTP client.
         *
         * @param timeoutSeconds specifies the request timeout seconds for HTTP requests.
         * @return this builder.
         */
        public Builder setRequestTimeout(int timeoutSeconds) {
            Preconditions.checkArgument(timeoutSeconds > 0, "The timeout seconds must be > 0.");
            _requestTimeoutSeconds = timeoutSeconds;
            return this;
        }

        public Integer getRequestTimeout() {
            return _requestTimeoutSeconds;
        }

        /**
         * Set the instance decorator which could be used to decorate job instances querying from this client.
         *
         * @param decorator specifies the {@link InstanceDecorator} which could be {@code null}.
         * @return this builder.
         */
        public Builder setInstanceDecorator(InstanceDecorator decorator) {
            _instanceDecorator = decorator;
            return this;
        }

        public  InstanceDecorator getInstanceDecorator() { return  _instanceDecorator; }
    }

    /**
     * The URI for the Cook scheduler endpoint.
     */
    private final URI _uri;

    /**
     * A kerberized HTTP client.
     */
    private final CloseableHttpClient _httpClient;

    /**
     * A {@link ScheduledExecutorService} for pulling job status and invoking listener.
     */
    private final ScheduledExecutorService _listenerService;

    /**
     * A map from job UUID to job which is an internal map for tracking active jobs, i.e. non successfully completed
     * jobs. Note that this map will be modified when<br>
     * -- a job is successfully submitted to Cook scheduler where a new map entry is added;<br>
     * -- and a job is successfully completed where old map entry is removed.
     */
    private final Map<UUID, Job> _activeUUIDToJob;

    /**
     * A map from job UUID to its associated {@link JobListener}. Note that the keys of the following map should be a
     * (possibly strict) subset of {@code _activeUUIDToJob} as some of jobs may not have {@link JobListener}s
     * associated.
     */
    private final Map<UUID, JobListener> _activeUUIDToListener;

    /**
     * The maximum number of jobs per any http request.
     */
    private int _batchRequestSize;

    /**
     * An interval in seconds to query job status update periodically.
     */
    private int _statusUpdateInterval;

    /**
     * The job instance decorator which will be used to decorate job instances when querying from this client.
     */
    private InstanceDecorator _instanceDecorator;

    private JobClient(String host, int port, String endpoint, int statusUpdateInterval, int batchSubmissionLimit,
            InstanceDecorator instanceDecorator, CloseableHttpClient httpClient) throws URISyntaxException {
        _statusUpdateInterval = statusUpdateInterval;
        _batchRequestSize = batchSubmissionLimit;
        _activeUUIDToJob = new ConcurrentHashMap<>();
        _activeUUIDToListener = new ConcurrentHashMap<>();
        _uri = new URIBuilder().setScheme("http").setHost(host).setPort(port).setPath(endpoint).build();
        _httpClient = httpClient;
        _log.info("Open ScheduledExecutorService for listener.");
        _listenerService = startListenService();
        _instanceDecorator = instanceDecorator;
    }

    @Override
    public void close() {
        _log.info("Close ScheduledExecutorService for listener.");
        _listenerService.shutdownNow();
    }

    private ScheduledExecutorService startListenService() {
        final ScheduledExecutorService scheduledExecutorService =
                Executors.newScheduledThreadPool(1,
                        new ThreadFactoryBuilder().setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                            @Override
                            public void uncaughtException(Thread t, Throwable e) {
                                _log.error("Can not handle exception for listener service.", e);
                            }
                        }).build());

        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // Simply return if there is no listener.
                if (_activeUUIDToListener.isEmpty()) {
                    return;
                }
                // Query active jobs
                ImmutableMap<UUID, Job> currentUUIDToJob;
                try {
                    currentUUIDToJob = query(_activeUUIDToJob.keySet());
                } catch (JobClientException e) {
                    // Catch and log
                    _log.warn("Failed to query job status for jobs " + _activeUUIDToJob.keySet(), e);
                    return;
                }

                // Invoke listeners and update maps.
                for (Map.Entry<UUID, Job> entry : currentUUIDToJob.entrySet()) {
                    UUID uuid = entry.getKey();
                    Job currentJob = entry.getValue();
                    if (!_activeUUIDToJob.get(uuid).equals(currentJob)) {
                        // Firstly, invoke listener if there is a listener associated to this job.
                        final JobListener listener = _activeUUIDToListener.get(uuid);
                        if (null != listener) {
                            // XXX It is completely debatable what should be the correct behavior
                            // when have an exception from listener and we have the following possible options:
                            // 1. throw a {@link DeathError} to kill the JVM;
                            // 2. do not update {@code _activeUUIDToJob} and keep retrying in the
                            // next cycle;
                            // 3. simply log the error but the listener will miss this status
                            // update (current behavior).
                            try {
                                listener.onStatusUpdate(currentJob);
                            } catch (Exception e) {
                                _log.warn("Failed to invoke listener onStatusUpdate() for " + currentJob
                                        + ". The listener service won't deliver this message again.", e);
                            }
                        }
                        // Secondly, update internal maps if necessary.
                        if (currentJob.getStatus() != Job.Status.COMPLETED) {
                            _activeUUIDToJob.put(uuid, currentJob);
                        } else {
                            _activeUUIDToJob.remove(uuid);
                            _activeUUIDToListener.remove(uuid);
                        }
                    }
                }
            }
        }, _statusUpdateInterval, _statusUpdateInterval, TimeUnit.SECONDS);
        return scheduledExecutorService;
    }

    /**
     * Submit a list of jobs to Cook scheduler. It will <br>
     * -- firstly associate each job with the provided {@link JobListener}<br>
     * -- secondly submit these jobs to Cook scheduler and track them until they complete.
     *
     * @param jobs The list of jobs expected to submit.
     * @param listener specifies an instance of {@link JobListener} listening all job status updates.
     * @throws JobClientException
     */
    public void submit(List<Job> jobs, JobListener listener)
        throws JobClientException {
        // It is ok to change the listeners map even if the actual submission fails.
        for (Job job : jobs) {
            _activeUUIDToListener.put(job.getUUID(), listener);
        }
        submit(jobs);
    }

    private JobClientException releaseAndCreateException(HttpRequestBase httpRequest, final String msg, final Throwable cause) {
        if (null != httpRequest) {
            httpRequest.releaseConnection();
        }
        if (null != cause) {
            return new JobClientException(msg, cause);
        } else {
            return new JobClientException(msg);
        }
    }

    /**
     * Generate a HTTP POST request for a given uri and a JSON object.
     *
     * @param uri {@link URI} specifies the uri for the request.
     * @param params {@link JSONObject} specifies the parameters used in the HTTP POST request.
     * @return {@link HttpPost} request.
     * @throws URISyntaxException
     */
    public static HttpPost makeHttpPost(URI uri, JSONObject params) {
        try {
            StringEntity input = new StringEntity(params.toString());
            input.setContentType("application/json");
            HttpPost request = new HttpPost(uri);
            request.setEntity(input);
            return request;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Submit a list of jobs to Cook scheduler and start to track these jobs until they complete. Note that jobs
     * submitted through this API will not be listened by any listener.
     *
     * @param jobs specifies a list of {@link Job}s to be submitted.
     * @return the response string from Cook scheduler rest endpoint.
     * @throws JobClientException
     */
    public void submit(List<Job> jobs)
        throws JobClientException {
        JSONObject json;
        try {
            json = Job.jsonizeJob(jobs);
        } catch (JSONException e) {
            throw new JobClientException("Can not jsonize jobs to submit.", e);
        }
        HttpResponse httpResponse;
        HttpRequestBase httpRequest = makeHttpPost(_uri, json);
        try {
            httpResponse = executeWithRetries(httpRequest, 5, 10);
        } catch (IOException e) {
            throw releaseAndCreateException(httpRequest, "Can not submit POST request " + json + " via uri " + _uri, e);
        }

        // Get the response string.
        StatusLine statusLine = httpResponse.getStatusLine();
        HttpEntity entity = httpResponse.getEntity();
        if (entity == null) {
            throw releaseAndCreateException(httpRequest, "The response entity is null!", null);
        }
        String response = null;
        try {
            response = EntityUtils.toString(entity);
            // Ensure that the entity content has been fully consumed and the underlying stream has been closed.
            EntityUtils.consume(entity);
        } catch (ParseException | IOException e) {
            throw releaseAndCreateException(httpRequest, "Can not parse the response for POST request " + json +
                    " via uri " + _uri, e);
        }
        if (_log.isDebugEnabled()) {
            _log.debug("Response String for submitting jobs" + json.toString() + " is " + response);
        }

        // Base on the decision graph
        // http://clojure-liberator.github.io/liberator/tutorial/decision-graph.html
        // If the jobs are submitted successfully, the status code is 201.
        // If a job uses a UUID which has been used before, the returned status code is 400 and the
        // return message is something like:
        // clojure.lang.ExceptionInfo: UUID 26719da8-194f-44f9-9e6d-8a17500f5109 already used {:uuid
        // #uuid "26719da8-194f-44f9-9e6d-8a17500f5109"}

        // A flag to indicate if the submission is successful.
        boolean isSuccess = false;
        if (null != statusLine && statusLine.getStatusCode() == HttpStatus.SC_CREATED) {
            isSuccess = true;
            _log.info("Successfully execute POST request with data " + json + " via uri " + _uri);
        } else if (null != statusLine && statusLine.getStatusCode() >= HttpStatus.SC_BAD_REQUEST) {
            final Pattern patternUUID =
                   Pattern.compile("[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12} already used");
            final Matcher matchUUID = patternUUID.matcher(response);
            if (matchUUID.find()) {
                _log.info("Successfully execute POST request with several retries " + json + " via uri " + _uri);
                isSuccess = true;
            } else {
                _log.warn("Failed to execute POST request with several retries " + json + " via uri " + _uri);
            }
        }
        if (null != httpRequest) {
            httpRequest.releaseConnection();
        }
        if (isSuccess) {
            // Update status map.
            for (Job job : jobs) {
                _activeUUIDToJob.put(job.getUUID(), job);
            }
        } else {
            _log.error("Failed to submit jobs " + json.toString());
            throw new JobClientException("The response of POST request " + json + " via uri " + _uri + ": "
                    + statusLine.getReasonPhrase() + ", " + statusLine.getStatusCode());
        }
    }

    /**
     * Query jobs for a given list of job {@link UUID}s. If the size of the list is larger that the
     * {@code _batchRequestSize}, it will partition the list into smaller lists for query and return all query results
     * together.
     *
     * @param uuids specifies a list of job {@link UUID}s expected to query.
     * @return a {@link ImmutableMap} from job {@link UUID} to {@link Job}.
     * @throws JobClientException
     */
    public ImmutableMap<UUID, Job> query(Collection<UUID> uuids)
        throws JobClientException {
        final List<NameValuePair> allParams = new ArrayList<NameValuePair>(uuids.size());
        for (UUID uuid : uuids) {
            allParams.add(new BasicNameValuePair("job", uuid.toString()));
        }
        final ImmutableMap.Builder<UUID, Job> UUIDToJob = ImmutableMap.builder();
        // Partition a large query into small queries.
        for (final List<NameValuePair> params : Lists.partition(allParams, _batchRequestSize)) {
            HttpResponse httpResponse;
            HttpRequestBase httpRequest;
            try {
                URIBuilder uriBuilder = new URIBuilder(_uri);
                uriBuilder.addParameters(params);
                httpRequest = new HttpGet(uriBuilder.build());
                httpResponse = _httpClient.execute(httpRequest);
            } catch (IOException | URISyntaxException e) {
                throw releaseAndCreateException(null, "Can not submit GET request " + params + " via uri " + _uri, e);
            }
            // Check status code.
            final StatusLine statusLine = httpResponse.getStatusLine();
            // Base on the decision graph
            // http://clojure-liberator.github.io/liberator/tutorial/decision-graph.html
            // The status code for the proper GET response is 200.
            if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
                throw releaseAndCreateException(httpRequest, "The response of GET request " + params + " via uri " + _uri + ": "
                        + statusLine.getReasonPhrase() + ", " + statusLine.getStatusCode(), null);
            }
            // Parse the response.
            String response = null;
            try {
                // parse the response to string.
                final HttpEntity entity = httpResponse.getEntity();
                response = EntityUtils.toString(entity);
                // Ensure that the entity content has been fully consumed and the underlying stream has been closed.
                EntityUtils.consume(entity);
                for (Job job : Job.parseFromJSON(response, _instanceDecorator)) {
                    UUIDToJob.put(job.getUUID(), job);
                }
            } catch (JSONException | ParseException | IOException e) {
                throw new JobClientException("Can not parse the response = " + response + " for GET request " + params +
                        " via uri " + _uri, e);
            } finally {
                httpRequest.releaseConnection();
            }
        }
        return UUIDToJob.build();
    }

    /**
     * Abort jobs for a given list of job {@link UUID}s. If the size of the list is larger that the
     * {@code _batchRequestSize}, it will partition the list into smaller lists to abort separately.
     *
     * @param uuids specifies a list of job {@link UUID}s expected to abort.
     * @throws JobClientException
     */
    public void abort(Collection<UUID> uuids)
        throws JobClientException {
        final List<NameValuePair> allParams = new ArrayList<NameValuePair>(uuids.size());
        for (UUID uuid : uuids) {
            allParams.add(new BasicNameValuePair("job", uuid.toString()));
        }
        // Partition a large query into small queries.
        for (final List<NameValuePair> params : Lists.partition(allParams, _batchRequestSize)) {
            HttpRequestBase httpRequest;
            try {
                URIBuilder uriBuilder = new URIBuilder(_uri);
                uriBuilder.addParameters(params);
                httpRequest =  new HttpDelete(uriBuilder.build());
            } catch (URISyntaxException e) {
                throw releaseAndCreateException(null, "Can not submit DELETE request " + params + " via uri " + _uri, e);
            }
            HttpResponse httpResponse;
            try {
                httpResponse = _httpClient.execute(httpRequest);
            } catch (IOException e) {
                throw releaseAndCreateException(httpRequest, "Can not submit DELETE request " + params + " via uri " + _uri, e);
            }
            // Check status code.
            final StatusLine statusLine = httpResponse.getStatusLine();
            // Base on the decision graph
            // http://clojure-liberator.github.io/liberator/tutorial/decision-graph.html
            // If jobs are aborted successfully, the returned status code is 204.
            if (statusLine.getStatusCode() != HttpStatus.SC_NO_CONTENT) {
                throw releaseAndCreateException(httpRequest, "The response of DELETE request " + params + " via uri " + _uri + ": "
                        + statusLine.getReasonPhrase() + ", " + statusLine.getStatusCode(), null);
            }
            // Parse the response.
            try {
                // Parse the response to string.
                final HttpEntity entity = httpResponse.getEntity();
                if (null != entity) {
                    final String response = EntityUtils.toString(entity);
                    if (_log.isDebugEnabled()) {
                        _log.debug("Response String for aborting jobs " + uuids + " is " + response);
                    }
                }
            } catch (ParseException | IOException e) {
                throw new JobClientException("Can not parse the response for DELETE request " + params + " via uri "
                        + _uri, e);
            } finally {
                httpRequest.releaseConnection();
            }
        }
    }

    /**
     * A wrapper for the function {@code execute(HttpRequestBase request)}. It retries using
     * exponential retry strategy with the following intervals:<br>
     * {@code baseIntervalSeconds, baseIntervalSeconds * 2, baseIntervalSeconds * 2^2, baseIntervalSeconds * 2^3 ...}
     *
     * @param request {@link HttpRequestBase} specifies the HTTP request expected to execute.
     * @param maxRetries specifies the maximum number of retries.
     * @param baseIntervalSeconds specifies the interval base for the exponential retry strategy
     * @return the {@link HttpResponse} if the execution is successful with maximum number of
     *         retries.
     * @throws IOException
     */
    public HttpResponse executeWithRetries(HttpRequestBase request, int maxRetries, long baseIntervalSeconds)
        throws IOException {
        Preconditions.checkArgument(maxRetries > 0, "maxRetries must be > 1");
        Preconditions.checkArgument(baseIntervalSeconds > 0, "baseIntervalSeconds must be > 0");

        HttpResponse response = null;
        IOException exception = null;
        long sleepMillis = TimeUnit.SECONDS.toMillis(baseIntervalSeconds);
        for (int i = 0; i < maxRetries; ++i) {
            try {
                response = _httpClient.execute(request);
            } catch (IOException e) {
                exception = e;
                response = null;
                try {
                    Thread.sleep(sleepMillis);
                    sleepMillis *= 2;
                } catch (InterruptedException ie) {
                    // no-op
                }
            }
            if (null != response) {
                return response;
            }
        }
        // If it can not get any response after several retries, re-throw the the exception.
        throw new IOException(exception);
    }

    @Override
    public String toString() {
        return "JobClient [_uri=" + _uri + ", _httpClient=" + _httpClient + ", _listenerService=" + _listenerService
                + ", _activeUUIDToJob=" + _activeUUIDToJob + ", _batchSubmissionLimit=" + _batchRequestSize
                + ", _statusUpdateInterval=" + _statusUpdateInterval + "]";
    }
}
