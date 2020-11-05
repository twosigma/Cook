package com.twosigma.cook.jobclient;

import org.json.JSONException;
import org.json.JSONObject;

public class Disk {
    /**
     *   request - disk space guaranteed for the job
     *   limit - max disk space the job can use
     *   type - type of disk for the job
     */
    private Double _request;
    private Double _limit;
    private String _type;

    public enum DiskType {
        STANDARD ("standard"), PD_SSD ("pd-ssd");

        private final String typeString;

        /**
         * @param typeString
         */
        DiskType(final String typeString) {
            this.typeString = typeString;
        }

        /* (non-Javadoc)
         * @see java.lang.Enum#toString()
         */
        @Override
        public String toString() {
            return typeString;
        }
    }

    public Disk() {
        this(null, null, null);
    }

    public Disk(Double request, Double limit, String type) {
        this._request = request;
        this._limit = limit;
        this._type = type;
    }

    public void setRequest(Double request) { this._request = request; }

    public void setLimit(Double limit) {
        this._limit = limit;
    }

    public void setType(String type) {
        this._type = type;
    }

    public void setType(DiskType type) {
        setType(type.toString());
    }

    public Double getRequest() { return _request; }
    public Double getLimit() { return _limit; }
    public String getType() { return _type; }

    /**
     * Function to determine if disk request should be included in the JSON for the Cook payload
     */
    public boolean shouldIncludeInJSON() {
        return (this._request != null || this._limit != null || this._type != null);
    }

    public JSONObject toJSONObject() throws JSONException {
        final JSONObject object = new JSONObject();
        if(this._request != null) {
            object.put("request", _request);
        }
        if (this._limit != null) {
            object.put("limit", _limit);
        }
        if (this._type != null) {
            object.put("type", _type);
        }
        return object;
    }

    public static Disk parseFromJSON(JSONObject object) throws JSONException {
        Disk newDisk = new Disk();
        if (object.has("request")) {
            Double request =  object.getDouble("request");
            newDisk.setRequest(request);
        }
        if (object.has("limit")) {
            Double limit =  object.getDouble("limit");
            newDisk.setLimit(limit);
        }
        if (object.has("type")) {
            String type =  object.getString("type");
            newDisk.setType(type);
        }
        return newDisk;
    }
}
