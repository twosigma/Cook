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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * An interface for the Cook job client.
 */
public interface JobClientInterface {

    /**
     * Submit a list of jobs to Cook scheduler. It will <br>
     * -- firstly associate each job with the provided {@link JobListener}<br>
     * -- secondly submit these jobs to Cook scheduler and track them until they complete.<br>
     * Behaves identically to {@link #submit(List) submit(jobs)} if <code>listener</code> is <code>null</code>.
     *
     * @param jobs The list of jobs expected to submit.
     * @param listener specifies an instance of {@link JobListener} listening all job status updates.
     * @throws JobClientException
     */
    public void submit(List<Job> jobs, JobListener listener)
        throws JobClientException;

    /**
     * Submits jobs and groups to Cook scheduler and start to track the jobs until they complete. Note that jobs
     * submitted through this API will not be listened by any listener.
     *
     * @param jobs specifies a list of {@link Job}s to be submitted.
     * @param groups specifies a list of {@link Group}s to be submitted.
     * @return the response string from Cook scheduler rest endpoint.
     * @throws JobClientException
     */
    public void submitWithGroups(List<Job> jobs, List<Group> groups)
        throws JobClientException;

    /**
     * Submit a list of jobs and groups to Cook scheduler. It will <br>
     * -- firstly associate each group with the provided {@link JobListener}<br>
     * -- secondly submit these jobs to Cook scheduler and track them until they complete.<br>
     * Behaves identically to {@link #submitWithGroups(List, List) submit(jobs, groups)}
     * if <code>listener</code> is <code>null</code>.
     *
     * @param jobs The list of jobs to be submitted.
     * @param groups The list of groups to be submitted.
     * @param listener specifies an instance of {@link JobListener} listening all job status updates.
     * @throws JobClientException
     */
    public void submitWithGroups(List<Job> jobs, List<Group> groups, GroupListener listener)
        throws JobClientException;

    /**
     * Submit a list of jobs to Cook scheduler and start to track these jobs until they complete. Note that jobs
     * submitted through this API will not be listened by any listener.
     *
     * @param jobs specifies a list of {@link Job}s to be submitted.
     * @return the response string from Cook scheduler rest endpoint.
     * @throws JobClientException
     */
    public void submit(List<Job> jobs)
        throws JobClientException;

    /**
     * Query jobs for a given list of job {@link UUID}s. If the list size is larger that the
     * {@code _batchRequestSize}, it will partition the list into smaller lists and query them
     * respectively and return all query results together.
     *
     * @param uuids specifies a list of job {@link UUID}s expected to query.
     * @return a {@link ImmutableMap} from job {@link UUID} to {@link Job}.
     * @throws JobClientException
     */
    public Map<UUID, Job> queryJobs(Collection<UUID> uuids)
        throws JobClientException;

    /**
     * Query a group for a group's jobs.
     * @param group specifies a group, whose jobs will be queried.
     * @return a {@link ImmutableMap} from job {@link UUID} to {@link Job}.
     * @throws JobClientException
     */
    public Map<UUID, Job> queryGroupJobs(Group group)
        throws JobClientException;

    /**
     * Query a group for its status.
     * @param guuid specifies the group to be queried.
     * @return a {@link Group} status.
     * @throws JobClientException
     */
    public Group queryGroup(UUID guuid)
        throws JobClientException;

    /**
     * Query a collection of groups for their status.
     * @param guuids specifies the uuids of the {@link Group}s to be queried.
     * @return a map of {@link UUID}s to {@link Group}s.
     * @throws JobClientException
     */
    public Map<UUID, Group> queryGroups(Collection<UUID> guuids)
        throws JobClientException;

    /**
     * Abort jobs for a given list of job {@link UUID}s. If the size of the list is larger that the
     * {@code _batchRequestSize}, it will partition the list into smaller lists to abort separately.
     *
     * @param uuids specifies a list of job {@link UUID}s expected to abort.
     * @throws JobClientException
     */
    public void abort(Collection<UUID> uuids)
        throws JobClientException;

}
