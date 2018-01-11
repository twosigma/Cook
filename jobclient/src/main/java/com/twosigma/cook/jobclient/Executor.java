package com.twosigma.cook.jobclient;

/**
 * Enum representing valid options for the executor field in a job and instance.
 */

public enum Executor {
    COOK,
    EXECUTOR;

    public static Executor fromString(final String name) {
        for (final Executor executor : values()) {
            if (executor.name().toLowerCase().equals(name)) {
                return executor;
            }
        }
        return null;
    }

    public String displayName() {
        return name().toLowerCase();
    }
}
