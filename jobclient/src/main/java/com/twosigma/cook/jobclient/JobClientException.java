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
 * Job client exception.
 * <p>
 * Created: March 14, 2015
 *
 * @author wzhao
 */
public class JobClientException extends Exception {
    private static final long serialVersionUID = 1L;

    private final Integer httpResponseCode;

    JobClientException(final String msg) {
        this(msg, (Integer) null);
    }

    JobClientException(final String msg, final Throwable cause) {
        this(msg, cause, null);
    }

    JobClientException(final String msg, final Integer httpResponseCode) {
        super(msg);
        this.httpResponseCode = httpResponseCode;
    }


    JobClientException(final String msg, final Throwable cause, final Integer httpResponseCode) {
        super(msg, cause);
        this.httpResponseCode = httpResponseCode;
    }

    public Integer getHttpResponseCode() {
        return httpResponseCode;
    }
}
