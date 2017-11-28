package com.twosigma.cook.jobclient.constraint.api;

public enum Operator {
    EQUALS("EQUALS");

    Operator(String name) {
    }

    /**
     * Parse an operator from its string representation.
     *
     * @param op specifies a string representation of operator.
     * @return an operator for the specified name.
     */
    public static Operator fromString(String op) {
        return Enum.valueOf(Operator.class, op.trim().toUpperCase());
    }
}