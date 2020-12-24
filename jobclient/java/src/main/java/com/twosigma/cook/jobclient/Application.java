package com.twosigma.cook.jobclient;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Applications scheduling jobs on Cook can optionally provide information about the
 * application, which could be used to analyze the source of requests after the fact.
 */
public class Application {

    final private String _name;
    final private String _version;
    final private String _workloadClass;
    final private String _workloadId;
    final private String _workloadDetails;

    public Application(String name, String version, String workloadClass, String workloadId, String workloadDetails) {
        this._name = name;
        this._version = version;
        this._workloadClass = workloadClass;
        this._workloadId = workloadId;
        this._workloadDetails = workloadDetails;
    }

    public Application(String name, String version) {
        this(name, version, null, null, null);
    }

    public String getName() {
        return _name;
    }

    public String getVersion() {
        return _version;
    }

    public String getWorkloadClass() {
        return _workloadClass;
    }

    public String getWorkloadId() {
        return _workloadId;
    }

    public String getWorkloadDetails() {
        return _workloadDetails;
    }

    static JSONObject jsonizeApplication(Application application) throws JSONException {
        final JSONObject object = new JSONObject();
        object.put("name", application._name);
        object.put("version", application._version);
        if (application._workloadClass != null) {
            object.put("workload-class", application._workloadClass);
        }
        if (application._workloadId != null) {
            object.put("workload-id", application._workloadId);
        }
        if (application._workloadDetails != null) {
            object.put("workload-details", application._workloadDetails);
        }
        return object;
    }

    private static String tryGetString(JSONObject object, String key) throws JSONException {
        return object.has(key) ? object.getString(key) : null;
    }

    static Application parseFromJSON(JSONObject object) throws JSONException {
        String name = object.getString("name");
        String version = object.getString("version");
        String workloadClass = tryGetString(object, "workload-class");
        String workloadId = tryGetString(object,"workload-id");
        String workloadDetails = tryGetString(object,"workload-details");
        return new Application(name, version, workloadClass, workloadId, workloadDetails);
    }
}
