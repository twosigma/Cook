package com.twosigma.cook.kubernetes;

import com.google.common.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.CoreV1Event;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;

public class WatchHelper {

    public static Watch<V1Pod> createPodWatch(ApiClient apiClient, String resourceVersion) throws ApiException {
        CoreV1Api api = new CoreV1Api(apiClient);
        return Watch.createWatch(apiClient,
                api.listPodForAllNamespacesCall(null, null, null, null, null, null,
                        resourceVersion, null, null, true, null),
                new TypeToken<Watch.Response<V1Pod>>() {}.getType());
    }

    public static Watch<V1Node> createNodeWatch(ApiClient apiClient, String resourceVersion) throws ApiException {
        CoreV1Api api = new CoreV1Api(apiClient);
        return Watch.createWatch(apiClient,
                api.listNodeCall(null, null, null, null, null, null, resourceVersion, null, null, true, null),
                new TypeToken<Watch.Response<V1Node>>() {}.getType());
    }

    public static Watch<CoreV1Event> createEventWatch(ApiClient apiClient, String resourceVersion) throws ApiException {
        CoreV1Api api = new CoreV1Api(apiClient);
        return Watch.createWatch(apiClient,
                api.listEventForAllNamespacesCall(null, null, null, null, null,
                        null, resourceVersion, null, null, true, null),
                new TypeToken<Watch.Response<CoreV1Event>>() {}.getType());
    }
}