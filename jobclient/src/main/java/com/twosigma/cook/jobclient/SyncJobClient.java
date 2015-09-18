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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
import org.apache.http.client.methods.*;
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

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class SyncJobClient implements Closeable {

    private static final Logger _log = Logger.getLogger(SyncJobClient.class);

    /**
     * A builder for the {@link AsyncJobClient}.
     */
    public static class Builder
    {
        private String _host;

        private String _endpoint;

        private Integer _port;


        public static final int DEFAULT_BATCH_REQUEST_SIZE = 32;

        public static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 60;


        /**
         * The number of jobs per http request for job submission or status query.
         */
        private Integer _batchRequestSize;

        /**
         * The timeout in seconds for all http requests.
         */
        private Integer _requestTimeoutSeconds;

        private HttpClientBuilder _httpClientBuilder;

        public Builder() {
            _httpClientBuilder = HttpClientBuilder.create();
        }

        /**
         * Prior to {@code Build()}, host, port, and endpoint for the Cook scheduler must be specified either by user or
         * properties.
         *
         * @return a {@link AsyncJobClient}.
         * @throws URISyntaxException
         */
        public SyncJobClient build() throws URISyntaxException {
            // host, port, and endpoint MUST be specified either by user

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
            return new SyncJobClient(
                    Preconditions.checkNotNull(_host, "host must be set"),
                    Preconditions.checkNotNull(_port, "port must be set"),
                    Preconditions.checkNotNull(_endpoint, "endpoint must be set"),
                    _batchRequestSize,
                    _httpClientBuilder.build());
        }

        public Builder setUsernameAuth(String username, String password) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            _httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return this;
        }

        public Builder setKerberosAuth() {
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
            credentialsProvider.setCredentials(AuthScope.ANY, creds);

            _httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            _httpClientBuilder.setDefaultAuthSchemeRegistry(RegistryBuilder.<AuthSchemeProvider>create()
                    .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build());
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
    }

    /**
     * The URI for the Cook scheduler endpoint.
     */
    private final URI _uri;

    /**
     * An HTTP client.
     */
    private final CloseableHttpClient _httpClient;

    /**
     * The maximum number of jobs per any http request.
     */
    private int _batchRequestSize;

    /**
     * @param host
     * @param port
     * @param endpoint
     * @throws URISyntaxException
     */
    private SyncJobClient(String host, int port, String endpoint, int batchSubmissionLimit,
                           CloseableHttpClient httpClient) throws URISyntaxException
    {
        _batchRequestSize = batchSubmissionLimit;
        _uri = new URIBuilder().setScheme("http").setHost(host).setPort(port).setPath(endpoint).build();
        _httpClient = httpClient;
    }

    @Override
    public void close() {
    }

/*

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
*/
    static <T extends HttpRequestBase> CloseableHttpRequest<T> wrapCloseable(T request) {
        return new CloseableHttpRequest<>(request);
    }

    /**
     * Generate a HTTP POST request for a given uri and a JSON object.
     *
     * @param uri {@link URI} specifies the uri for the request.
     * @param params {@link JSONObject} specifies the parameters used in the HTTP POST request.
     * @return {@link HttpPost} request.
     */
    public static CloseableHttpRequest<HttpPost> makeHttpPost(URI uri, JSONObject params) {
        try {
            StringEntity input = new StringEntity(params.toString());
            input.setContentType("application/json");
            HttpPost request = new HttpPost(uri);
            request.setEntity(input);
            return wrapCloseable(request);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static CloseableHttpRequest<HttpGet> makeHttpGet(URI uri, List<NameValuePair> params)
            throws URISyntaxException
    {
        URIBuilder uriBuilder = new URIBuilder(uri);
        uriBuilder.addParameters(params);
        return wrapCloseable(new HttpGet(uriBuilder.build()));
    }

    private static CloseableHttpRequest<HttpDelete> makeHttpDelete(URI _uri, List<NameValuePair> params)
        throws URISyntaxException
    {
        URIBuilder uriBuilder = new URIBuilder(_uri);
        uriBuilder.addParameters(params);
        return wrapCloseable(new HttpDelete(uriBuilder.build()));
    }


    /**
     * Submit a list of jobs to Cook scheduler and start to track these jobs until they complete. Note that jobs
     * submitted through this API will not be listened by any listener.
     *
     * @param jobs specifies a list of {@link Job}s to be submitted.
     * @throws JobClientException
     */
    public void submit(List<Job> jobs)
            throws JobClientException
    {
        JSONObject json;
        try {
            json = Job.jsonizeJob(jobs);
        } catch (JSONException e) {
            throw new JobClientException("Can not jsonize jobs to submit.", e);
        }

        try (CloseableHttpRequest<HttpPost> request = makeHttpPost(_uri, json);
             CloseableHttpResponse httpResponse = executeWithRetries(request.get(), 5, 10))
        {

            // Get the response string.
            StatusLine statusLine = httpResponse.getStatusLine();
            HttpEntity entity = httpResponse.getEntity();
            if (entity == null) {
                throw new JobClientException("The response entity is null!", null);
            }
            String response;
            try {
                response = EntityUtils.toString(entity);
                // Ensure that the entity content has been fully consumed and the underlying stream has been closed.
                EntityUtils.consume(entity);
            } catch (ParseException | IOException e) {
                throw new JobClientException("Can not parse the response for POST request " + json + " via uri " + _uri, e);
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
            if (!isSuccess) {
                _log.error("Failed to submit jobs " + json.toString());
                throw new JobClientException("The response of POST request " + json + " via uri " + _uri + ": "
                        + statusLine);
            }


        } catch (IOException e) {
            throw new JobClientException("Can not submit POST request " + json + " via uri " + _uri, e);
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
        final List<NameValuePair> allParams = new ArrayList<>(uuids.size());
        for (UUID uuid : uuids) {
            allParams.add(new BasicNameValuePair("job", uuid.toString()));
        }
        final ImmutableMap.Builder<UUID, Job> UUIDToJob = ImmutableMap.builder();
        // Partition a large query into small queries.
        for (final List<NameValuePair> params : Lists.partition(allParams, _batchRequestSize)) {
            try (CloseableHttpRequest<HttpGet> request = makeHttpGet(_uri, params);
                CloseableHttpResponse httpResponse = _httpClient.execute(request.get()))
            {
                // Check status code.
                final StatusLine statusLine = httpResponse.getStatusLine();
                // Base on the decision graph
                // http://clojure-liberator.github.io/liberator/tutorial/decision-graph.html
                // The status code for the proper GET response is 200.
                if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
                    throw new JobClientException("The response of GET request " + params + " via uri " + _uri + ": "
                            + statusLine.getReasonPhrase() + ", " + statusLine.getStatusCode(), null);
                }
                // Parse the response.
                try {
                    // parse the response to string.
                    final HttpEntity entity = httpResponse.getEntity();
                    final String response = EntityUtils.toString(entity);
                    // Ensure that the entity content has been fully consumed and the underlying stream has been closed.
                    EntityUtils.consume(entity);
                    for (Job job : Job.parseFromJSON(response)) {
                        UUIDToJob.put(job.getUUID(), job);
                    }
                } catch (JSONException | ParseException | IOException e) {
                    throw new JobClientException("Can not parse the response for GET request " + params + " via uri "
                            + _uri, e);
                }
            } catch (IOException | URISyntaxException e) {
                throw new JobClientException( "Can not submit GET request " + params + " via uri " + _uri, e);
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
        final List<NameValuePair> allParams = new ArrayList<>(uuids.size());
        for (UUID uuid : uuids) {
            allParams.add(new BasicNameValuePair("job", uuid.toString()));
        }
        // Partition a large query into small queries.
        for (final List<NameValuePair> params : Lists.partition(allParams, _batchRequestSize)) {
            try (CloseableHttpRequest<HttpDelete> delete = makeHttpDelete(_uri, params);
                 CloseableHttpResponse httpResponse = _httpClient.execute(delete.get()))
            {
                // Check status code.
                final StatusLine statusLine = httpResponse.getStatusLine();
                // Base on the decision graph
                // http://clojure-liberator.github.io/liberator/tutorial/decision-graph.html
                // If jobs are aborted successfully, the returned status code is 204.
                if (statusLine.getStatusCode() != HttpStatus.SC_NO_CONTENT) {
                    throw new JobClientException("The response of DELETE request " + params + " via uri " + _uri + ": "
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
                }

            } catch (IOException | URISyntaxException e) {
                throw new JobClientException("Can not submit DELETE request " + params + " via uri " + _uri, e);
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
    public CloseableHttpResponse executeWithRetries(HttpRequestBase request, int maxRetries, long baseIntervalSeconds)
        throws IOException
    {
        Preconditions.checkArgument(maxRetries > 0, "maxRetries must be > 1");
        Preconditions.checkArgument(baseIntervalSeconds > 0, "baseIntervalSeconds must be > 0");

        CloseableHttpResponse response;
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
        return "AsyncJobClient [_uri=" + _uri
                + ", _httpClient=" + _httpClient
                + ", _batchSubmissionLimit=" + _batchRequestSize
                + "]";
    }
}
