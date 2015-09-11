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


package com.twosigma.cook.jobclient.httpclient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
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
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.json.JSONObject;

import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;

/**
 * A simple HTTP client for kerberized rest endpoints.
 * <p>
 * Created: March 14, 2015
 *
 * @author wzhao
 */
public class BasicKerberizedHttpClient
{
    private static final Logger _log = Logger.getLogger(BasicKerberizedHttpClient.class);

    private final String _host;

    private final int _port;

    private final CloseableHttpClient _httpClient;

    /**
     * Construct a client for kerberized rest endpoints.
     * 
     * @param host {@link String} specifies the server host expected to connect.
     * @param port specifies the server port expected to connect.
     */
    public BasicKerberizedHttpClient(String host, int port, int requestTimeoutSeconds) {
        _host = host;
        _port = port;
        HttpClientBuilder clientBuilder = HttpClientBuilder.create();

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(_host, _port, AuthScope.ANY_REALM, AuthSchemes.SPNEGO),
                new Credentials() {
                    @Override
                    public String getPassword() {
                        return null;
                    }

                    @Override
                    public Principal getUserPrincipal() {
                        return null;
                    }
                });

        clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        clientBuilder.setDefaultAuthSchemeRegistry(RegistryBuilder.<AuthSchemeProvider>create()
                .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build());
        // Create the request retry handler with a retry count of 3, requestSentRetryEnabled false
        // and using the following list of non-retriable IOException classes:
        // InterruptedIOException UnknownHostException ConnectException SSLException
        clientBuilder.setRetryHandler(new StandardHttpRequestRetryHandler());
        // When timeout, throw {@link IOException}s like {@link SocketTimeoutException}, {@link
        // ConnectionTimeoutException} etc.
        RequestConfig requestConfig =
                RequestConfig.custom().setSocketTimeout(requestTimeoutSeconds * 1000)
                        .setConnectTimeout(requestTimeoutSeconds * 1000)
                        .setConnectionRequestTimeout(requestTimeoutSeconds * 1000).setStaleConnectionCheckEnabled(true)
                        .build();
        clientBuilder.setDefaultRequestConfig(requestConfig);
        _httpClient = clientBuilder.build();
    }

    /**
     * Generate a HTTP GET request for a given uri and a list of name / value pairs.
     * 
     * @param uri {@link URI} specifies the uri for the request.
     * @param params specifies a list of {@link NameValuePair} name / value pairs parameters used as
     *        elements of HTTP messages.
     * @return a {@link HttpGet} request.
     * @throws URISyntaxException
     */
    public static HttpGet getHttpGet(URI uri, List<NameValuePair> params) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(uri);
        uriBuilder.addParameters(params);
        return new HttpGet(uriBuilder.build());
    }

    /**
     * Generate a HTTP DELETE request for a given uri and a list of name / value pairs.
     * 
     * @param uri {@link URI} specifies the uri for the request.
     * @param params specifies a list of {@link NameValuePair} name / value pairs parameters used as
     *        elements of HTTP messages.
     * @return {@link HttpDelete} request.
     * @throws URISyntaxException
     */
    public static HttpDelete getHttpDelete(URI uri, List<NameValuePair> params)
            throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(uri);
        uriBuilder.addParameters(params);
        return new HttpDelete(uriBuilder.build());
    }

    /**
     * Generate a HTTP POST request for a given uri and a list of name / value pairs.
     * 
     * @param uri {@link URI} specifies the uri for the request.
     * @param params specifies a list of {@link NameValuePair} name / value pairs parameters used as
     *        elements of HTTP messages.
     * @return {@link HttpPost} request.
     * @throws URISyntaxException
     */
    public static HttpPost getHttpPost(URI uri, List<NameValuePair> params)
            throws URISyntaxException, UnsupportedEncodingException {
        HttpPost request = new HttpPost(uri);
        request.setEntity(new UrlEncodedFormEntity(params));
        return request;
    }

    /**
     * Generate a HTTP POST request for a given uri and a JSON object.
     * 
     * @param uri {@link URI} specifies the uri for the request.
     * @param params {@link JSONObject} specifies the parameters used in the HTTP POST request.
     * @return {@link HttpPost} request.
     * @throws URISyntaxException
     */
    public static HttpPost getHttpPost(URI uri, JSONObject params)
            throws UnsupportedEncodingException {
        StringEntity input = new StringEntity(params.toString());
        input.setContentType("application/json");
        HttpPost request = new HttpPost(uri);
        request.setEntity(input);
        return request;
    }

    /**
     * Generate a HTTP POST request for a given uri and a JSON object.
     * <p>
     * This is a wrapper function for {@code getHttpPost(URI uri, JSONObject params)} without throwing out any
     * exception.
     * 
     * @param uri {@link URI} specifies the uri for the request.
     * @param params {@link JSONObject} specifies the parameters used in the HTTP POST request.
     * @return {@link HttpPost} request.
     */
    public static HttpPost getHttpPostNoThrow(URI uri, JSONObject params) {
        Preconditions.checkNotNull(params, "params should not be null.");
        try {
            return getHttpPost(uri, params);
        } catch (UnsupportedEncodingException e) {
            // It is very unlikely to have issue to encode the string from the JSONObject.
            _log.error("Failed to get HTTP post request for " + params.toString(), e);
            return null;
        }
    }

    /**
     * Execute HTTP request through the delegated client.
     * 
     * @param request {@link HttpRequestBase} specifies the HTTP request expected to execute.
     * @return the {@link HttpResponse} after execution.
     * @throws IOException
     */
    public HttpResponse execute(HttpRequestBase request) throws IOException {
        return _httpClient.execute(request);
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
                response = execute(request);
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
}
