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

import org.apache.http.auth.AuthScheme;
import org.apache.http.impl.auth.SPNegoScheme;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 * An extended {@link SPNegoSchemeFactory} which allows to produce customized {@link SPNegoScheme}s
 * that allow to use passed in GSS credential to generate GSS token.
 *
 * <p>
 * Created: January 14, 2016
 *
 * @author wzhao
 */
public class BasicSPNegoSchemeFactory extends SPNegoSchemeFactory {

    /**
     * The canonical name lookup done by the HttpClient does not always give the value
     * expected by the underlying Kerberos library, leading to hostname mismatch errors.
     * We disable canonicalization here and allow Kerberos to do its own resolution.
     */
    public static final boolean USE_CANONICAL_HOSTNAME = false;

    public static SPNegoSchemeFactory build(final boolean stripPort, final GSSCredentialProvider credentialProvider) {
        return credentialProvider == null
            // USE_CANONICAL_HOSTNAME should be enabled below for HttpClient v4.4 or above
            ? new SPNegoSchemeFactory(true /*, USE_CANONICAL_HOSTNAME */)
            : new BasicSPNegoSchemeFactory(true, credentialProvider);
    }

    /**
     * In the apache.httpclient 4.3.x version, {@code SPNEGO_OID = "1.3.6.1.5.5.2"}. However, we are
     * using {@code SPNEGO_OID = "1.2.840.113554.1.2.2"} here.
     *
     * @see <a
     *      href="https://github.com/apache/httpclient/blob/4.3.x/httpclient/src/main/java/org/apache/http/impl/auth/SPNegoScheme.java">
     *      https://github.com/apache/httpclient/blob/4.3.x/httpclient/src/main/java/org/apache/http/impl/auth/SPNegoScheme.java </a>
     */
    static final String SPNEGO_OID = "1.2.840.113554.1.2.2";

    /**
     * A extended {@link SPNegoScheme} which provide the ability to use provided credential.
     */
    private static class BasicSPNegoScheme extends SPNegoScheme {

        private Oid _oid;
        private final GSSCredentialProvider _credentialProvider;

        private synchronized Oid getOID() throws GSSException {
            if (_oid == null) {
                _oid = new Oid(SPNEGO_OID);
            }
            return _oid;
        }

        private GSSCredential getCredential() {
            if (_credentialProvider == null) {
                return null;
            }
            return _credentialProvider.getCredential();
        }

        BasicSPNegoScheme(final boolean stripPort, final GSSCredentialProvider credentialProvider) {
            // USE_CANONICAL_HOSTNAME should be enabled below for HttpClient v4.4 or above
            super(stripPort /*, USE_CANONICAL_HOSTNAME */);
            _credentialProvider = credentialProvider;
        }

        /**
         * @see <a
         *      href="https://github.com/apache/httpclient/blob/4.3.x/httpclient/src/main/java/org/apache/http/impl/auth/GGSSchemeBase.java">
         *      https://github.com/apache/httpclient/blob/4.3.x/httpclient/src/main/java/org/apache/http/impl/auth/GGSSchemeBase.java </a>
         */
        @Override
        protected byte[] generateToken(final byte[] input, final String authServer)
                throws GSSException {
            byte[] token = input;
            if (token == null) {
                token = new byte[0];
            }
            final GSSManager manager = getManager();
            final GSSName serverName =
                    manager.createName("HTTP@" + authServer, GSSName.NT_HOSTBASED_SERVICE);
            final GSSCredential cred = getCredential();
            final Oid oid = getOID();
            // XXX this line is the major difference comparing to the base class in this method.
            // Here, it uses the provided GSS credential instead of null. In the later version of
            // apache.httpclient, e.g. 4.5.x, there should be a better way to do this.
            final GSSContext gssContext =
                    manager.createContext(serverName.canonicalize(oid), oid, cred,
                            GSSContext.DEFAULT_LIFETIME);
            gssContext.requestMutualAuth(true);
            gssContext.requestConf(true);
            // XXX the following is commented out comparing to the base class in this method.
            // gssContext.requestCredDeleg(true);
            return gssContext.initSecContext(token, 0, token.length);
        }
    }

    private final GSSCredentialProvider _credentialProvider;

    protected BasicSPNegoSchemeFactory(final boolean stripPort, final GSSCredentialProvider credentialProvider) {
        // USE_CANONICAL_HOSTNAME should be enabled below for HttpClient v4.4 or above
        super(stripPort /*, USE_CANONICAL_HOSTNAME */);
        _credentialProvider = credentialProvider;
    }

    @Override
    public AuthScheme newInstance(final HttpParams params) {
        return new BasicSPNegoScheme(isStripPort(), _credentialProvider);
    }

    @Override
    public AuthScheme create(final HttpContext context) {
        return new BasicSPNegoScheme(isStripPort(), _credentialProvider);
    }
}
