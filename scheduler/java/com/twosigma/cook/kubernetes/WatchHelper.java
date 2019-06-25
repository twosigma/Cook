package com.twosigma.cook.kubernetes;

import com.google.common.reflect.TypeToken;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Node;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.util.Watch;

public class WatchHelper {

    public static Watch<V1Pod> createPodWatch(ApiClient apiClient, String resourceVersion) throws ApiException {
        CoreV1Api api = new CoreV1Api(apiClient);
        return Watch.createWatch(apiClient,
                api.listPodForAllNamespacesCall(null, null, null, null, null, null,
                        resourceVersion, null, true, null, null),
                new TypeToken<Watch.Response<V1Pod>>() {}.getType());
    }

    public static Watch<V1Node> createNodeWatch(ApiClient apiClient, String resourceVersion) throws ApiException {
        CoreV1Api api = new CoreV1Api(apiClient);
        return Watch.createWatch(apiClient,
                api.listNodeCall(null, null, null, null, null,
                        null, resourceVersion, null, true, null,
                        null),
                new TypeToken<Watch.Response<V1Node>>() {}.getType());
    }
}