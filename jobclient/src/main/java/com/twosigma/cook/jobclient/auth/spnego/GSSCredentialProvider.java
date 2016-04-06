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

package com.twosigma.cook.jobclient.auth.spnego;

import org.ietf.jgss.GSSCredential;

/**
 * A simple {@link GSSCredential} provider could be used to hold or provide the latest valid
 * credential.
 * <p>
 * Created: January 14, 2016
 *
 * @author wzhao
 */
public class GSSCredentialProvider {
    private GSSCredential _credential = null;

    /**
     * @return the {@link GSSCredential} held in this provider. If there is no credential held in
     *         this hold, it will simply return null.
     */
    public synchronized GSSCredential getCredential() {
        return _credential;
    }

    /**
     * @return update {@link GSSCredential} held in this provider.
     */
    public synchronized void setCredential(GSSCredential credential) {
        _credential = credential;
    }

    /**
     * Clean the {@link GSSCredential} held in this provider.
     */
    public synchronized void clear() {
        _credential = null;
    }
}
