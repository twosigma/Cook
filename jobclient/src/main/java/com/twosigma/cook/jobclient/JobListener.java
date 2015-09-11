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


/**
 * Interface for job listener.
 * <p>
 * Created: March 14, 2015
 *
 * @author wzhao
 */
public interface JobListener {
    /**
     * The following method will be invoked in any of the following job status transitions:
     * INITIALIZED -> WAITING, WAITING -> RUNNING, RUNNING -> COMPLETED where it will receive a
     * {@link Job} object with a possible status WAITING, RUNNING and COMPLETED respectively.
     * <p>
     * Note that if any exception when {@link JobClient} invokes this method for a job status
     * update, it will just simply log this exception. It won't invoke this method for the
     * particular status update again.
     * 
     * @param job
     */
    public void onStatusUpdate(Job job);
}
