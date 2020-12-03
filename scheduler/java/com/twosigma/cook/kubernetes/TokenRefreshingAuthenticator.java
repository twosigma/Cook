package com.twosigma.cook.kubernetes;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.credentials.Authentication;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Similar to TokenFileAuthenticator in the official Kubernetes library. It takes a supplier that generates a
 * token and automatically calls it when a new token is needed. Ideally this should be solved at the OpenAPI layer when https://github.com/OpenAPITools/openapi-generator/pull/6036 is fixed. For now, hijack the HTTP request. */
public class TokenRefreshingAuthenticator implements Authentication, Interceptor {
    private static final long tokenLifeTimeSeconds = TimeUnit.MINUTES.toSeconds(10);

    private final Supplier<String> tokenSupplier;
    private Instant expiry;
    private String token;

    /** Return a token authenticator that automatically refreshes when less than 10 minutes old.
     *
     * @param tokenSupplier The token supplier. If this this is null or returns null, the interceptor does not
     *                      add an Authorization Header.
     *
     * */
    public TokenRefreshingAuthenticator(Supplier<String> tokenSupplier) {
        this.tokenSupplier = tokenSupplier;
        this.expiry = Instant.MIN;
    }

    /**
     * Get an updated authorization token for this token
     */
    private String getToken() {
        if (tokenSupplier != null)
            synchronized (tokenSupplier) {
                Instant now = Instant.now();
                if (now.isAfter(this.expiry)) {
                    token = tokenSupplier.get();
                    expiry = now.plusSeconds(tokenLifeTimeSeconds);
                }
                return this.token;
            }
        return null;
    }

    @Override
    public void provide(ApiClient apiClient) {
        OkHttpClient withInterceptor = apiClient.getHttpClient().newBuilder().addInterceptor(this).build();
        apiClient.setHttpClient(withInterceptor);
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();
        token = getToken();
        if (token != null) {
            Request newRequest = request.newBuilder()
                    .header("Authorization", "Bearer " + token).build();
            return chain.proceed(newRequest);
        } else {
            return chain.proceed(request);
        }
    }

    /** From a KubeConfig, crate a TokenRefreshingAuthenticator */
    public static TokenRefreshingAuthenticator fromKubeConfig(KubeConfig kubeconfig) {
        return new TokenRefreshingAuthenticator(kubeconfig::getAccessToken);
    }
}