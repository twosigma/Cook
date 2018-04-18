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
import java.util.HashSet;
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
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twosigma.cook.jobclient.auth.spnego.BasicSPNegoSchemeFactory;
import com.twosigma.cook.jobclient.auth.spnego.GSSCredentialProvider;

/**
 * An implementation for the Cook job client.
 * <p>
 * This client supports the following three key operations<br>
 * -- submit: submit jobs and groups to Cook;<br>
 * -- query: query jobs and groups status along with their instances from Cook;<br>
 * -- abort: abort jobs and groups from Cook.<br>
 * <p>
 * Note that this client only tracks jobs and groups submitted through it. Periodically, it queries the Cook scheduler
 * rest endpoint for job status updates. If any job or group status changes, it will<br>
 * -- update internal map from UUID to job/group object;<br>
 * -- invoke listener call back method.<br>
 * <p>
 * Also note that, each job could potentially be associated with a different {@link JobListener} respectively. However,
 * different jobs could reference the same job listener.
 * <p>
 * Created: March 14, 2015
 *
 * @author wzhao
 */
public class JobClient implements Closeable, JobClientInterface {

    private static final Logger _log = Logger.getLogger(JobClient.class);

    protected static final String COOK_IMPERSONATE_HEADER = "X-Cook-Impersonate";

    /**
     * A builder for the {@link JobClient}.
     */
    public static class Builder
    {
        private String _host;

        private String _jobEndpoint;

        private String _groupEndpoint;

        private Integer _port;

        public static final int DEFAULT_STATUS_UPDATE_INTERVAL_SECONDS = 10;

        public static final int DEFAULT_BATCH_REQUEST_SIZE = 32;

        public static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 60;

        public static final int DEFAULT_SUBMIT_RETRY_INTERVAL_SECONDS = 10;

        /**
         * An interval in seconds which will be used to query job status update periodically.
         */
        private Integer _statusUpdateIntervalSeconds;

        /**
         * An interval in seconds to retry job submits.
         */
        private Integer _submitRetryIntervalSeconds;

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
         * Prior to {@code Build()}, host, port, and endpoint must be specified.
         *
         * @return a {@link JobClient}.
         * @throws URISyntaxException
         */
        public JobClient build() throws URISyntaxException {
            // The definition of the following parameters are optional.
            if (_statusUpdateIntervalSeconds == null) {
                _statusUpdateIntervalSeconds = DEFAULT_STATUS_UPDATE_INTERVAL_SECONDS;
            }
            if (_submitRetryIntervalSeconds == null) {
                _submitRetryIntervalSeconds = DEFAULT_SUBMIT_RETRY_INTERVAL_SECONDS;
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
                    Preconditions.checkNotNull(_jobEndpoint, "jobEndpoint must be set"),
                    _groupEndpoint,
                    _statusUpdateIntervalSeconds,
                    _submitRetryIntervalSeconds,
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

            _httpClientBuilder.setDefaultAuthSchemeRegistry(RegistryBuilder.<AuthSchemeProvider> create()
                    .register(AuthSchemes.SPNEGO, authSchemaProvider).build());
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
         * Deprecated, backwards-compatible version of setJobEndpoint.
         * @param jobEndpoint {@link String} specifies the Cook scheduler endpoint.
         * @return this builder.
         */
        public Builder setEndpoint(String jobEndpoint) {
            return this.setJobEndpoint(jobEndpoint);
        }


        /**
         * Set the Cook scheduler endpoint where the job client expected to build will send the requests to.
         *
         * @param jobEndpoint {@link String} specifies the Cook scheduler endpoint.
         * @return this builder.
         */
        public Builder setJobEndpoint(String jobEndpoint) {
            if (!jobEndpoint.startsWith("/")) {
                _jobEndpoint = "/" + jobEndpoint;
            } else {
                _jobEndpoint = jobEndpoint;
            }
            return this;
        }

        /**
         * Set the Cook scheduler endpoint where the job client will send requests about groups.
         *
         * @param groupEndpoint {@link String} specifies the Cook scheduler group endpoint.
         * @return this builder.
         */
        public Builder setGroupEndpoint(String groupEndpoint) {
            if (!groupEndpoint.startsWith("/")) {
                _groupEndpoint = "/" + groupEndpoint;
            } else {
                _groupEndpoint = groupEndpoint;
            }
            return this;
        }

        public String getEndpoint() {
            return _jobEndpoint;
        }

        public String getJobEndpoint() {
            return _jobEndpoint;
        }

        public String getGroupEndpoint() {
            return _groupEndpoint;
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

        public Integer getSubmitRetryInterval() {
            return _submitRetryIntervalSeconds;
        }

        public Builder setSubmitRetryInterval(int submitRetryIntervalSeconds) {
            this._submitRetryIntervalSeconds = submitRetryIntervalSeconds;
            return this;
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
     * The URI for the Cook scheduler job endpoint.
     */
    private final URI _jobURI;

    /**
     * The URI for the Cook scheduler group endpoint.
     */
    private final URI _groupURI;

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
    private final Map<UUID, JobListener> _jobUUIDToListener;

    /**
     * A map from group UUID to group which is an internal map for tracking active groups, i.e. non successfully
     * completed groups. Note that this map will be modified when<br>
     * -- a group is successfully submitted to Cook scheduler where a new map entry is added;<br>
     * -- and a group is successfully completed where old map entry is removed.
     */
    private final Map<UUID, Group> _activeUUIDToGroup;

    /**
     * A map from job group UUID to its associated {@link GroupListener}. Note that the keys of the following map should
     * be a (possibly strict) subset of {@code _activeUUIDToGroup} as some of jobs may not have {@link GroupListener}s
     * associated.
     */
    private final Map<UUID, GroupListener> _groupUUIDToListener;

    /**
     * The maximum number of jobs per any http request.
     */
    private int _batchRequestSize;

    /**
     * An interval in seconds to query job status update periodically.
     */
    private int _statusUpdateInterval;

    /**
     * An interval in seconds to retry job submits.
     */
    private int _submitRetryInterval;

    /**
     * The job instance decorator which will be used to decorate job instances when querying from this client.
     */
    private InstanceDecorator _instanceDecorator;

    private JobClient(String host, int port, String jobEndpoint, String groupEndpoint, int statusUpdateInterval,
                      int submitRetryInterval, int batchSubmissionLimit, InstanceDecorator instanceDecorator,
                      CloseableHttpClient httpClient) throws URISyntaxException {
        _statusUpdateInterval = statusUpdateInterval;
        _submitRetryInterval = submitRetryInterval;
        _batchRequestSize = batchSubmissionLimit;
        _activeUUIDToJob = new ConcurrentHashMap<>();
        _jobUUIDToListener = new ConcurrentHashMap<>();
        _activeUUIDToGroup = new ConcurrentHashMap<>();
        _groupUUIDToListener = new ConcurrentHashMap<>();
        _jobURI = new URIBuilder().setScheme("http").setHost(host).setPort(port).setPath(jobEndpoint).build();
        if (groupEndpoint != null) {
            _groupURI = new URIBuilder().setScheme("http").setHost(host).setPort(port).setPath(groupEndpoint).build();
        } else {
            _groupURI = null;
        }
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
                // Process Jobs and JobListeners first
                // Simply return if there is no listener.
                if (!_jobUUIDToListener.isEmpty()) {
                    // Query active jobs
                    Map<UUID, Job> currentUUIDToJob;
                    try {
                        currentUUIDToJob = queryJobs(_activeUUIDToJob.keySet());
                    } catch (JobClientException e) {
                        // Catch and log
                        _log.warn("Failed to query job status for jobs " + _activeUUIDToJob.keySet(), e);
                        return;
                    }

                    // Invoke listeners and update maps for job
                    for (Map.Entry<UUID, Job> entry : currentUUIDToJob.entrySet()) {
                        UUID juuid = entry.getKey();
                        Job currentJob = entry.getValue();
                        if (!_activeUUIDToJob.get(juuid).equals(currentJob)) {
                            // Firstly, invoke job listener if there is a listener associated to this job.
                            final JobListener listener = _jobUUIDToListener.get(juuid);
                            if (listener != null) {
                                // XXX It is completely debatable what should be the correct behavior here
                                // when a listener throws an exception. We have the following possible options:
                                // 1. simply propagate the exception;
                                // 2. keep {@code _activeUUIDToJob} being unchanged and retrying in the next cycle;
                                // 3. simply log the error but the listener will miss this status
                                // update (which is the current behavior).
                                try {
                                    listener.onStatusUpdate(currentJob);
                                } catch (Exception e) {
                                    _log.warn("Failed to invoke listener onStatusUpdate() for " + currentJob
                                            + ". The listener service won't deliver this message again.", e);
                                }
                            }

                            // Secondly, update internal maps if necessary.
                            if (currentJob.getStatus() != Job.Status.COMPLETED) {
                                _activeUUIDToJob.put(juuid, currentJob);
                            } else {
                                _activeUUIDToJob.remove(juuid);
                                _jobUUIDToListener.remove(juuid);
                            }
                        }
                    }
                }
                if (!_groupUUIDToListener.isEmpty()) {
                    // Now process Groups and GroupListeners
                    // Query active groups
                    Map<UUID, Group> currentUUIDToGroup;
                    try {
                        currentUUIDToGroup = queryGroups(_activeUUIDToGroup.keySet());
                    } catch (JobClientException e) {
                        // Catch and log
                        _log.warn("Failed to query group status for groups " + _activeUUIDToGroup.keySet(), e);
                        return;
                    }
                    // Invoke listeners and update maps for groups
                    for (Map.Entry<UUID, Group> entry : currentUUIDToGroup.entrySet()) {
                        UUID guuid = entry.getKey();
                        Group currentGroup = entry.getValue();
                        if (!_activeUUIDToGroup.get(guuid).equals(currentGroup)) {
                            final GroupListener listener = _groupUUIDToListener.get(guuid);
                            if (listener != null) {
                            // Invoke group listeners
                                try {
                                    listener.onStatusUpdate(currentGroup);
                                } catch (Exception e) {
                                    _log.warn("Failed to invoke listener onStatusUpdate() for " + currentGroup
                                            + ". The listener service won't deliver this message again.", e);
                                }
                            }

                            // Secondly, update internal maps if necessary.
                            if (currentGroup.getStatus() != Group.Status.COMPLETED) {
                                _activeUUIDToGroup.put(guuid, currentGroup);
                            } else {
                                _activeUUIDToGroup.remove(guuid);
                                _groupUUIDToListener.remove(guuid);
                            }
                        }
                    }
                }
            }
        }, _statusUpdateInterval, _statusUpdateInterval, TimeUnit.SECONDS);
        return scheduledExecutorService;
    }

    @Override
    public void submit(List<Job> jobs, JobListener listener)
        throws JobClientException {
        submit(jobs, listener, null);
    }

    @Override
    public void submitWithGroups(List<Job> jobs, List<Group> groups)
        throws JobClientException {
        submitWithGroups(jobs, groups, null, null);
    }

    /**
     * @see #submitWithGroups(List, List, GroupListener)
     */
    private void submitWithGroups(List<Job> jobs, List<Group> groups, GroupListener listener, String impersonatedUser)
        throws JobClientException {
        // It is ok to change the listeners map even if the actual submission fails because it won't
        // update the internal status map {@code _activeUUIDTOJob}.
        if (listener != null) {
            for (Group group : groups) {
                _groupUUIDToListener.put(group.getUUID(), listener);
            }
        }

        JSONObject json = new JSONObject();
        try {
            JSONObject groupsJSON = Group.jsonizeGroups(groups);
            JSONObject jobsJSON = Job.jsonizeJob(jobs);
            json.put("groups", groupsJSON.getJSONArray("groups"));
            json.put("jobs", jobsJSON.getJSONArray("jobs"));
        } catch (JSONException e) {
            throw new JobClientException("Can not jsonize jobs or groups to submit.", e);
        }
        HttpResponse httpResponse;
        HttpRequestBase httpRequest = makeHttpPost(_jobURI, json, impersonatedUser);

        try {
            httpResponse = executeWithRetries(httpRequest, 5, 10);
        } catch (IOException e) {
            throw releaseAndCreateException(httpRequest, null, "Can not submit POST request " + json + " via uri " + _jobURI, e);
        }

        // Get the response string.
        StatusLine statusLine = httpResponse.getStatusLine();
        HttpEntity entity = httpResponse.getEntity();
        if (entity == null) {
            throw releaseAndCreateException(httpRequest, null, "The response entity is null!", null);
        }
        String response = null;
        try {
            response = EntityUtils.toString(entity);
            // Ensure that the entity content has been fully consumed and the underlying stream has been closed.
            EntityUtils.consume(entity);
        } catch (ParseException | IOException e) {
            throw releaseAndCreateException(httpRequest, null, "Can not parse the response for POST request " + json +
                    " via uri " + _jobURI, e);
        }
        if (_log.isDebugEnabled()) {
            _log.debug("Response String for submitting jobs and groups" + json.toString() + " is " + response);
        }

        // Base on the decision graph
        // http://clojure-liberator.github.io/liberator/tutorial/decision-graph.html
        // If the jobs and groups are submitted successfully, the status code is 201.
        // If a job or group uses a UUID which has been used before, the returned status code is 400 and the
        // return message is something like:
        // clojure.lang.ExceptionInfo: [Job | Group] UUID 26719da8-194f-44f9-9e6d-8a17500f5109 already used {:uuid
        // #uuid "26719da8-194f-44f9-9e6d-8a17500f5109"}

        // A flag to indicate if the submission is successful.
        boolean isSuccess = false;
        if (null != statusLine && statusLine.getStatusCode() == HttpStatus.SC_CREATED) {
            isSuccess = true;
            _log.info("Successfully execute POST request with data " + json + " via uri " + _jobURI);
        } else if (null != statusLine && statusLine.getStatusCode() >= HttpStatus.SC_BAD_REQUEST) {
            final Pattern patternUUID =
                   Pattern.compile("([a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12} already used)");
            final Matcher matchUUID = patternUUID.matcher(response);
            if (matchUUID.find()) {
                _log.info("Successfully execute POST request with several retries " + json + " via uri " + _jobURI);
                isSuccess = true;
            } else {
                _log.warn("Failed to execute POST request with several retries " + json + " via uri " + _jobURI);
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
            for (Group group : groups) {
                _activeUUIDToGroup.put(group.getUUID(), group);
            }
        } else {
            _log.error("Failed to submit jobs " + json.toString());
            throw releaseAndCreateException(httpRequest, httpResponse, "The response of POST request " + json + " via uri " + _jobURI + ": "
                    + statusLine.getReasonPhrase() + ", " + statusLine.getStatusCode() + " Body is " + response, null);
        }
    }

    @Override
    public void submitWithGroups(List<Job> jobs, List<Group> groups, GroupListener listener)
        throws JobClientException {
        submitWithGroups(jobs, groups, listener, null);
    }

    private JobClientException releaseAndCreateException(HttpRequestBase httpRequest, HttpResponse httpResponse, final String msg, final Throwable cause) {
        StringBuilder newMsg = new StringBuilder(msg);
        if (null != httpRequest) {
            httpRequest.releaseConnection();
        }
        if (null != httpResponse) {
            try {
                newMsg.append(" Response body: " + EntityUtils.toString(httpResponse.getEntity()));
            } catch (IOException e) {
            }
        }
        if (null != cause) {
            return new JobClientException(newMsg.toString(), cause);
        } else {
            return new JobClientException(newMsg.toString());
        }
    }

    private JobClientException groupEndpointMissingException(final String msg) {
        return new JobClientException(msg);
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
        return makeHttpPost(uri, params, null);
    }

    private static <R extends HttpRequestBase> R addImpersonation(R request, String impersonatedUser) {
        if (impersonatedUser != null) {
            request.addHeader(COOK_IMPERSONATE_HEADER, impersonatedUser);
        }
        return request;
    }

    /**
     * @see #makeHttpPost(URI, JSONObject)
     */
    private static HttpPost makeHttpPost(URI uri, JSONObject params, String impersonatedUser) {
        try {
            StringEntity input = new StringEntity(params.toString());
            input.setContentType("application/json");
            HttpPost request = new HttpPost(uri);
            request.setEntity(input);
            addImpersonation(request, impersonatedUser);
            return request;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void submit(List<Job> jobs)
        throws JobClientException {
        submit(jobs, null, null);
    }

    /**
     * @see #submit(List, JobListener).
     */
    private void submit(List<Job> jobs, JobListener listener, String impersonatedUser)
        throws JobClientException {
        // It is ok to change the listeners map even if the actual submission fails because it won't
        // update the internal status map {@code _activeUUIDTOJob}.
        if (listener != null) {
            for (Job job : jobs) {
                _jobUUIDToListener.put(job.getUUID(), listener);
            }
        }

        JSONObject json;
        try {
            json = Job.jsonizeJob(jobs);
        } catch (JSONException e) {
            throw new JobClientException("Can not jsonize jobs to submit.", e);
        }
        HttpResponse httpResponse;
        HttpRequestBase httpRequest = makeHttpPost(_jobURI, json, impersonatedUser);

        try {
            httpResponse = executeWithRetries(httpRequest, 5, _submitRetryInterval);
        } catch (IOException e) {
            throw releaseAndCreateException(httpRequest, null, "Can not submit POST request " + json + " via uri " + _jobURI, e);
        }

        // Get the response string.
        StatusLine statusLine = httpResponse.getStatusLine();
        HttpEntity entity = httpResponse.getEntity();
        if (entity == null) {
            throw releaseAndCreateException(httpRequest, null, "The response entity is null!", null);
        }
        String response = null;
        try {
            response = EntityUtils.toString(entity);
            // Ensure that the entity content has been fully consumed and the underlying stream has been closed.
            EntityUtils.consume(entity);
        } catch (ParseException | IOException e) {
            throw releaseAndCreateException(httpRequest, null, "Can not parse the response for POST request " + json +
                    " via uri " + _jobURI, e);
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
            _log.info("Successfully execute POST request with data " + json + " via uri " + _jobURI);
        } else if (null != statusLine && statusLine.getStatusCode() >= HttpStatus.SC_INTERNAL_SERVER_ERROR) {
            final String transactionTimeoutMessage = "Transaction timed out.";
            if (String.valueOf(response).contains(transactionTimeoutMessage)) {
                _log.warn("POST experienced transaction timeout via uri " + _jobURI);
                Collection<UUID> jobUuids = new HashSet<>();
                for (Job job : jobs) {
                    jobUuids.add(job.getUUID());
                }

                _log.info("Sleeping " + _submitRetryInterval + " secs to allow transaction opportunity to complete");
                try {
                    Thread.sleep(_submitRetryInterval * 1000);
                } catch (InterruptedException e) {
                    // ignore
                }
                _log.info("Verifying whether all the jobs were created despite the transaction timeout message");
                try {
                    final Map<UUID, Job> uuidToJob = queryJobs(jobUuids);
                    if (uuidToJob.size() == jobUuids.size()) {
                        _log.info("All " + uuidToJob.size() + " jobs were created despite the transaction timeout message");
                        isSuccess = true;
                    } else {
                        _log.warn("POST failed: " + uuidToJob.size() + " of " + jobUuids.size() +
                            " jobs were created in the timed out transaction");
                    }
                } catch (Exception ex) {
                    _log.error("POST failed: all queried jobs were not found: " + ex.getMessage());
                }
            }
        } else if (null != statusLine && statusLine.getStatusCode() >= HttpStatus.SC_BAD_REQUEST) {
            final Pattern patternUUID =
                Pattern.compile("[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12} already used");
            final Matcher matchUUID = patternUUID.matcher(response);
            if (matchUUID.find()) {
                _log.info("Successfully execute POST request with several retries " + json + " via uri " + _jobURI);
                isSuccess = true;
            } else {
                _log.warn("Failed to execute POST request with several retries " + json + " via uri " + _jobURI);
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
            throw new JobClientException("The response of POST request " + json + " via uri " + _jobURI + ": "
                    + statusLine.getReasonPhrase() + ", " + statusLine.getStatusCode() + ", response is: " + response);
        }
    }

    @Override
    public Map<UUID, Job> queryJobs(Collection<UUID> uuids)
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
                URIBuilder uriBuilder = new URIBuilder(_jobURI);
                uriBuilder.addParameters(params);
                httpRequest = new HttpGet(uriBuilder.build());
                httpResponse = _httpClient.execute(httpRequest);
            } catch (IOException | URISyntaxException e) {
                throw releaseAndCreateException(null, null, "Can not submit GET request " + params + " via uri " + _jobURI, e);
            }
            // Check status code.
            final StatusLine statusLine = httpResponse.getStatusLine();
            // Base on the decision graph
            // http://clojure-liberator.github.io/liberator/tutorial/decision-graph.html
            // The status code for the proper GET response is 200.
            if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
                throw releaseAndCreateException(httpRequest, httpResponse, "The response of GET request " + params + " via uri " + _jobURI + ": "
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
                        " via uri " + _jobURI, e);
            } finally {
                httpRequest.releaseConnection();
            }
        }
        return UUIDToJob.build();
    }

    /**
     * An alias for queryJobs, for backwards-compatibility. Please use queryJobs instead.
     * @deprecated Please use queryJobs
     * @param uuids specifies a list of job {@link UUID}s expected to query.
     * @return a {@link ImmutableMap} from job {@link UUID} to {@link Job}.
     * @throws JobClientException
     */
    public Map<UUID, Job> query(Collection<UUID> uuids)
        throws JobClientException {
        return queryJobs(uuids);
    }

    @Override
    public Map<UUID, Job> queryGroupJobs(Group group)
        throws JobClientException {
        ArrayList<UUID> uuids = new ArrayList<UUID>();
        for (UUID juuid : group.getJobs()) {
            uuids.add(juuid);
        }
        return queryJobs(uuids);
    }

    @Override
    public Group queryGroup(UUID guuid)
        throws JobClientException {
        if (_groupURI == null) {
            throw groupEndpointMissingException("Cannot query groups if the jobclient's group endpoint is null");
        }
        final List<NameValuePair> allParams = new ArrayList<NameValuePair>();
        allParams.add(new BasicNameValuePair("detailed", "true"));
        allParams.add(new BasicNameValuePair("uuid", guuid.toString()));

        Group result;
        HttpResponse httpResponse;
        HttpRequestBase httpRequest;
        try {
            URIBuilder uriBuilder = new URIBuilder(_groupURI);
            uriBuilder.addParameters(allParams);
            httpRequest = new HttpGet(uriBuilder.build());
            httpResponse = _httpClient.execute(httpRequest);
        } catch (IOException | URISyntaxException e) {
            throw releaseAndCreateException(null, null, "Can not submit GET request " + allParams + " via uri " + _jobURI, e);
        }

        // Check status code.
        final StatusLine statusLine = httpResponse.getStatusLine();
        // Base on the decision graph
        // http://clojure-liberator.github.io/liberator/tutorial/decision-graph.html
        // The status code for the proper GET response is 200.
        if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
            throw releaseAndCreateException(httpRequest, httpResponse, "The response of GET request " + allParams + " via uri " + _jobURI + ": "
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
            result = Group.parseFromJSON(response, _instanceDecorator).get(0);
        } catch (JSONException | ParseException | IOException | IndexOutOfBoundsException e) {
            throw new JobClientException("Can not parse the response = " + response + " for GET request " + allParams +
                    " via uri " + _jobURI, e);
        } finally {
            httpRequest.releaseConnection();
        }
        return result;
    }

    @Override
    public Map<UUID, Group> queryGroups(Collection<UUID> guuids)
        throws JobClientException {
        if (_groupURI == null) {
            throw groupEndpointMissingException("Cannot query groups if the jobclient's group endpoint is null");
        }
        final List<NameValuePair> allParams = new ArrayList<NameValuePair>(guuids.size());
        for (UUID guuid : guuids) {
            allParams.add(new BasicNameValuePair("uuid", guuid.toString()));
        }
        allParams.add(new BasicNameValuePair("detailed", "true"));
        final ImmutableMap.Builder<UUID, Group> UUIDToGroup = ImmutableMap.builder();
        // Partition a large query into small queries.
        for (final List<NameValuePair> params : Lists.partition(allParams, _batchRequestSize)) {
            HttpResponse httpResponse;
            HttpRequestBase httpRequest;
            try {
                URIBuilder uriBuilder = new URIBuilder(_groupURI);
                uriBuilder.addParameters(params);
                httpRequest = new HttpGet(uriBuilder.build());
                httpResponse = _httpClient.execute(httpRequest);
            } catch (IOException | URISyntaxException e) {
                throw releaseAndCreateException(null, null, "Can not submit GET request " + params + " via uri " + _jobURI, e);
            }
            // Check status code.
            final StatusLine statusLine = httpResponse.getStatusLine();
            // Base on the decision graph
            // http://clojure-liberator.github.io/liberator/tutorial/decision-graph.html
            // The status code for the proper GET response is 200.
            if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
                throw releaseAndCreateException(httpRequest, httpResponse, "The response of GET request " + params + " via uri " + _jobURI + ": "
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
                for (Group group : Group.parseFromJSON(response, _instanceDecorator)) {
                    UUIDToGroup.put(group.getUUID(), group);
                }
            } catch (JSONException | ParseException | IOException e) {
                throw new JobClientException("Can not parse the response = " + response + " for GET request " + params +
                        " via uri " + _jobURI, e);
            } finally {
                httpRequest.releaseConnection();
            }
        }
        return UUIDToGroup.build();

    }

    @Override
    public void abort(Collection<UUID> uuids)
        throws JobClientException {
        abort(uuids, null);
    }

    /**
     * @see #abort(Collection)
     */
    private void abort(Collection<UUID> uuids, String impersonatedUser)
        throws JobClientException {
        final List<NameValuePair> allParams = new ArrayList<NameValuePair>(uuids.size());
        for (UUID uuid : uuids) {
            allParams.add(new BasicNameValuePair("job", uuid.toString()));
        }
        // Partition a large query into small queries.
        for (final List<NameValuePair> params : Lists.partition(allParams, _batchRequestSize)) {
            HttpRequestBase httpRequest;
            try {
                URIBuilder uriBuilder = new URIBuilder(_jobURI);
                uriBuilder.addParameters(params);
                httpRequest =  new HttpDelete(uriBuilder.build());
                addImpersonation(httpRequest, impersonatedUser);
            } catch (URISyntaxException e) {
                throw releaseAndCreateException(null, null, "Can not submit DELETE request " + params + " via uri " + _jobURI, e);
            }
            HttpResponse httpResponse;
            try {
                httpResponse = executeWithRetries(httpRequest, 5, 10);
            } catch (IOException e) {
                throw releaseAndCreateException(httpRequest, null, "Can not submit DELETE request " + params + " via uri " + _jobURI, e);
            }
            // Check status code.
            final StatusLine statusLine = httpResponse.getStatusLine();
            // Base on the decision graph
            // http://clojure-liberator.github.io/liberator/tutorial/decision-graph.html
            // If jobs are aborted successfully, the returned status code is 204.
            if (statusLine.getStatusCode() != HttpStatus.SC_NO_CONTENT) {
                throw releaseAndCreateException(httpRequest, httpResponse, "The response of DELETE request " + params + " via uri " + _jobURI + ": "
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
                        + _jobURI, e);
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
        return "JobClient [_jobURI=" + _jobURI + ", _httpClient=" + _httpClient + ", _listenerService=" + _listenerService
                + ", _activeUUIDToJob=" + _activeUUIDToJob + ", _batchSubmissionLimit=" + _batchRequestSize
                + ", _statusUpdateInterval=" + _statusUpdateInterval + "]";
    }

    /**
     * Create a proxy to this {@link JobClient} for submitting requests on behalf of another user.
     * @param impersonatedUser specifies the authentication principal of the user to be impersonated.
     * @return a {@link JobClientInterface} that can perform impersonated requests.
     */
    public JobClientInterface impersonating(String impersonatedUser) {
        return new ImpersonationProxy(impersonatedUser);
    }

    /**
     * A proxy to the enclosing {@link JobClient},
     * which injects impersonation requirements into relevant requests.
     */
    private class ImpersonationProxy implements JobClientInterface {

        private final String _impersonatedUser;

        protected ImpersonationProxy(String impersonatedUser) {
            _impersonatedUser = impersonatedUser;
        }

        @Override
        public void submit(List<Job> jobs, JobListener listener)
            throws JobClientException {
            JobClient.this.submit(jobs, listener, _impersonatedUser);
        }

        @Override
        public void submitWithGroups(List<Job> jobs, List<Group> groups)
            throws JobClientException {
            JobClient.this.submitWithGroups(jobs, groups, null, _impersonatedUser);
        }

        @Override
        public void submitWithGroups(List<Job> jobs, List<Group> groups, GroupListener listener)
            throws JobClientException {
            JobClient.this.submitWithGroups(jobs, groups, listener, _impersonatedUser);
        }

        @Override
        public void submit(List<Job> jobs)
            throws JobClientException {
            JobClient.this.submit(jobs, null, _impersonatedUser);
        }

        @Override
        public Map<UUID, Job> queryJobs(Collection<UUID> uuids)
            throws JobClientException {
            return JobClient.this.queryJobs(uuids);
        }

        @Override
        public Map<UUID, Job> queryGroupJobs(Group group)
            throws JobClientException {
            return JobClient.this.queryGroupJobs(group);
        }

        @Override
        public Group queryGroup(UUID guuid)
            throws JobClientException {
            return JobClient.this.queryGroup(guuid);
        }

        @Override
        public Map<UUID, Group> queryGroups(Collection<UUID> guuids)
            throws JobClientException {
            return JobClient.this.queryGroups(guuids);
        }

        @Override
        public void abort(Collection<UUID> uuids)
            throws JobClientException {
            JobClient.this.abort(uuids, _impersonatedUser);
        }
    }
}
