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

    public Application(String name, String version) {
        this._name = name;
        this._version = version;
    }

    public String getName() {
        return _name;
    }

    public String getVersion() {
        return _version;
    }

    static JSONObject jsonizeApplication(Application application) throws JSONException {
        final JSONObject object = new JSONObject();
        object.put("name", application._name);
        object.put("version", application._version);
        return object;
    }

    static Application parseFromJSON(JSONObject object) throws JSONException {
        String name = object.getString("name");
        String version = object.getString("version");
        return new Application(name, version);
    }
}
