package com.twosigma.cook.kubernetes;

import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.PatchUtils;
import org.joda.time.DateTime;

import java.util.List;

public class FinalizerHelper {
    /** A finalizer that is attached to a pod to ensure that it is not GC'ed by K8s before cook
     * has had a chance to collect the completion result (success or failed) */
    static public final String collectResultsFinalizer = "cook/prevent-pod-gc";

    /** Remove the collectResultsFinalizer from a pod if it exists on a pod and the pod is morked for
     * deletion. */
    static public void removeFinalizer(ApiClient apiClient, V1Pod pod) throws ApiException {
        CoreV1Api api = new CoreV1Api(apiClient);

        DateTime deletionTimestamp = pod.getMetadata().getDeletionTimestamp();
        if (deletionTimestamp != null) {
            List<String> finalizers = pod.getMetadata().getFinalizers();
            if (finalizers != null) {
                for (int ii = 0; ii < finalizers.size(); ii++) {
                    if (collectResultsFinalizer.equals(finalizers.get(ii))) {
                        String jsonPatchStr = "[{\"op\": \"remove\", \"path\": \"/metadata/finalizers/" + ii + "\"}]";
                        String podName = pod.getMetadata().getName();
                        String namespaceName = pod.getMetadata().getNamespace();
                        PatchUtils.patch(
                                V1Pod.class,
                                () ->
                                        api.patchNamespacedPodCall(
                                                podName,
                                                namespaceName,
                                                new V1Patch(jsonPatchStr),
                                                null,
                                                null,
                                                null, // field-manager is optional
                                                null,
                                                null),
                                V1Patch.PATCH_FORMAT_JSON_PATCH,
                                apiClient);
                        return; // Early abort if we've found the finalizer.
                    }
                }
            }
        }
    }
}

