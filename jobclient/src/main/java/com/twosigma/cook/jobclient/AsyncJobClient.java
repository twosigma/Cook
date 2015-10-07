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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
public class AsyncJobClient implements Closeable {

    private static final Logger _log = Logger.getLogger(AsyncJobClient.class);

    /**
     * A builder for the {@link AsyncJobClient}.
     */
    public static class Builder
    {
        public static final int DEFAULT_STATUS_UPDATE_INTERVAL_SECONDS = 10;

        private final SyncJobClient.Builder _syncJobClientBuilder = new SyncJobClient.Builder();

        /**
         * An interval in seconds which will be used to query job status update periodically.
         */
        private Integer _statusUpdateIntervalSeconds;

        public Builder() {}

        /**
         * Prior to {@code Build()}, host, port, and endpoint for the Cook scheduler must be specified either by user or
         * properties.
         * 
         * @return a {@link AsyncJobClient}.
         * @throws URISyntaxException
         */
        public AsyncJobClient build() throws URISyntaxException {

            // host, port, and endpoint MUST be specified either by user

            // The definition of the following parameters are optional.
            if (_statusUpdateIntervalSeconds == null) {
                _statusUpdateIntervalSeconds = DEFAULT_STATUS_UPDATE_INTERVAL_SECONDS;
            }
            return new AsyncJobClient(_syncJobClientBuilder.build(), _statusUpdateIntervalSeconds);
        }

        public Builder setUsernameAuth(String username, String password) {
            _syncJobClientBuilder.setUsernameAuth(username, password);
            return this;
        }

        public Builder setKerberosAuth() {
            _syncJobClientBuilder.setKerberosAuth();
            return this;
        }

        /**
         * Set the Cook scheduler host where the job client expected to build will connect to.
         * 
         * @param host {@link String} specifies the Cook scheduler host.
         * @return this builder.
         */
        public Builder setHost(String host) {
            _syncJobClientBuilder.setHost(host);
            return this;
        }

        /**
         * Set the Cook scheduler port where the job client expected to build will connect to.
         * 
         * @param port specifies the Cook scheduler port.
         * @return this builder.
         */
        public Builder setPort(int port) {
            _syncJobClientBuilder.setPort(port);
            return this;
        }

        /**
         * Set the Cook scheduler endpoint where the job client expected to build will send the requests to.
         * 
         * @param endpoint {@link String} specifies the Cook scheduler endpoint.
         * @return this builder.
         */
        public Builder setEndpoint(String endpoint) {
            _syncJobClientBuilder.setEndpoint(endpoint);
            return this;
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
            _syncJobClientBuilder.setBatchRequestSize(batchRequestSize);
            return this;
        }

        /**
         * Set HTTP request timeout in seconds expected to set SocketTimeout, ConnectionTimeout, and
         * ConnectionRequestTimeout for the HTTP client.
         * 
         * @param timeoutSeconds specifies the request timeout seconds for HTTP requests.
         * @return this builder.
         */
        public Builder setRequestTimeout(int timeoutSeconds) {
            _syncJobClientBuilder.setRequestTimeout(timeoutSeconds);
            return this;
        }
    }

    /**
     * Synchronous Job Client that we use to decorate with async functionality
     */
    private final SyncJobClient _syncJobClient;

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
     * An interval in seconds to query job status update periodically.
     */
    private int _statusUpdateInterval;

    /**
     * @param syncJobClient
     * @param statusUpdateInterval
     */
    private AsyncJobClient(SyncJobClient syncJobClient, int statusUpdateInterval) throws URISyntaxException {
        _syncJobClient = syncJobClient;
        _activeUUIDToJob = new ConcurrentHashMap<>();
        _activeUUIDToListener = new ConcurrentHashMap<>();
        _statusUpdateInterval = statusUpdateInterval;
        _log.info("Open ScheduledExecutorService for listener");
        _listenerService = startListenService();
    }

    @Override
    public void close() {
        _syncJobClient.close();
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
     * @param jobs
     * @param listener
     * @throws JobClientException
     */
    public void submit(List<Job> jobs, JobListener listener)
        throws JobClientException
    {
        // It is ok to change the listeners map even if the actual submission fails.
        for (Job job : jobs) {
            _activeUUIDToListener.put(job.getUUID(), listener);
        }

        _syncJobClient.submit(jobs);

        // Update status map.
        for (Job job : jobs) {
            _activeUUIDToJob.put(job.getUUID(), job);
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
        throws JobClientException
    {
        _syncJobClient.submit(jobs);
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
        throws JobClientException
    {
        return _syncJobClient.query(uuids);
    }

    /**
     * Abort jobs for a given list of job {@link UUID}s. If the size of the list is larger that the
     * {@code _batchRequestSize}, it will partition the list into smaller lists to abort separately.
     *
     * @param uuids specifies a list of job {@link UUID}s expected to abort.
     * @throws JobClientException
     */
    public void abort(Collection<UUID> uuids)
        throws JobClientException
    {
        _syncJobClient.abort(uuids);
    }


    @Override
    public String toString() {
        return "AsyncJobClient [_syncJobClient=" + _syncJobClient
                + ", _listenerService=" + _listenerService
                + ", _activeUUIDToJob=" + _activeUUIDToJob
                + ", _statusUpdateInterval=" + _statusUpdateInterval + "]";
    }
}
