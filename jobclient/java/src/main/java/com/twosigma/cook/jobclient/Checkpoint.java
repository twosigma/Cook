package com.twosigma.cook.jobclient;

import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Schema for a configuration to enable checkpointing
 */
public class Checkpoint {
    /**
     *   auto - checkpointing code will select the best method
     *   periodic - periodically create a checkpoint
     *   preemption - checkpoint is created on preemption before the VM is stopped
     */
    public static enum Mode {
        auto, periodic, preemption;
    }

    /**
     * Schema for checkpointing options
     */
    public static class CheckpointOptions {
        final private Set<String> _preservePaths;

        public CheckpointOptions(Set<String> preservePaths) {
            this._preservePaths = preservePaths;
        }

        public Set<String> getPreservePaths() {
            return _preservePaths;
        }

        public JSONObject toJSONObject() throws JSONException {
            final JSONObject object = new JSONObject();
            object.put("preserve-paths", _preservePaths);
            return object;
        }

        public static CheckpointOptions parseFromJSON(JSONObject object) throws JSONException {
            Set<String> preservePaths = new HashSet<>();
            JSONArray jsonArray = object.getJSONArray("preserve-paths");
            for (int i = 0; i < jsonArray.length(); ++i) {
                preservePaths.add(jsonArray.getString(i));
            }
            return new CheckpointOptions(preservePaths);
        }
    }

    /**
     * Schema for periodic checkpointing options
     */
    public static class PeriodicCheckpointOptions {
        final private int _periodSec;

        public PeriodicCheckpointOptions(int periodSec) {
            this._periodSec = periodSec;
        }

        public int getPeriodSec() {
            return _periodSec;
        }

        public JSONObject toJSONObject() throws JSONException {
            final JSONObject object = new JSONObject();
            object.put("period-sec", _periodSec);
            return object;
        }

        public static PeriodicCheckpointOptions parseFromJSON(JSONObject object) throws JSONException {
            int periodSec = object.getInt("period-sec");
            return new PeriodicCheckpointOptions(periodSec);
        }
    }

    final private Mode _mode;
    final private CheckpointOptions _checkpointOptions;
    final private PeriodicCheckpointOptions _periodicCheckpointOptions;

    public Checkpoint(Mode mode, CheckpointOptions checkpointOptions, PeriodicCheckpointOptions periodicCheckpointOptions) {
        this._mode = mode;
        this._checkpointOptions = checkpointOptions;
        this._periodicCheckpointOptions = periodicCheckpointOptions;
    }

    public Mode getMode() {
        return _mode;
    }

    public CheckpointOptions getCheckpointOptions() {
        return _checkpointOptions;
    }

    public PeriodicCheckpointOptions getPeriodicCheckpointOptions() {
        return _periodicCheckpointOptions;
    }

    public JSONObject toJSONObject() throws JSONException {
        final JSONObject object = new JSONObject();
        object.put("mode", _mode.toString());
        if (_checkpointOptions != null) {
            object.put("options", _checkpointOptions.toJSONObject());
        }
        if (_periodicCheckpointOptions != null) {
            object.put("periodic-options", _periodicCheckpointOptions.toJSONObject());
        }
        return object;
    }

    public static Checkpoint parseFromJSON(JSONObject object) throws JSONException {
        Mode mode = Mode.valueOf(object.getString("mode"));
        CheckpointOptions checkpointOptions = null;
        PeriodicCheckpointOptions periodicCheckpointOptions = null;
        if (object.has("options")) {
            checkpointOptions = CheckpointOptions.parseFromJSON(object.getJSONObject("options"));
        }
        if (object.has("periodic-options")) {
            periodicCheckpointOptions = PeriodicCheckpointOptions.parseFromJSON(object.getJSONObject("periodic-options"));
        }
        return new Checkpoint(mode, checkpointOptions, periodicCheckpointOptions);
    }
}
