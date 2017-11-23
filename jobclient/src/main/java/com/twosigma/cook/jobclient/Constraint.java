/*
 * Copyright (c) Two Sigma Open Source, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twosigma.cook.jobclient;

import com.google.common.base.Preconditions;
import org.json.JSONArray;

/**
 * A simple class to represent a constraint.
 *
 * A constraint has 3 fields: an attribute, an operator, and a value.
 * An example of constraints could be: "host","EQUALS", "foo.bar.com".
 *
 * For now, only "EQUALS" operator is supported by Cook. One could construct
 * a constraint with an "EQUALS" operator by
 *
 * <pre>
 *  Constraint c = Constraint.EQUALS.apply("host", "foo.bar.com");
 * </pre>
 */
final public class Constraint {

    public enum Operator {
        EQUALS("EQUALS");

        Operator(String name) {
        }

        /**
         * Returns a constraint by "applying" this operator to the given attribute and value.
         *
         * @param attribute The constraint attribute
         * @param value     The constraint value
         * @return a constraint.
         */
        public Constraint apply(String attribute, String value) {
            return new Constraint(attribute, this, value);
        }

        /**
         * Parse an operator from its string representation.
         *
         * @param op specifies a string representation of operator.
         * @return an operator for the specified name.
         */
        public static Constraint.Operator fromString(String op) {
            return Enum.valueOf(Constraint.Operator.class, op.trim().toUpperCase());
        }
    }

    private String _attribute;
    private Operator _operator;
    private String _value;

    Constraint(String attribute, Operator operator, String value) {
        Preconditions.checkNotNull(attribute);
        Preconditions.checkNotNull(operator);
        Preconditions.checkNotNull(value);
        _attribute = attribute;
        _operator = operator;
        _value = value;
    }

    Constraint(String attribute, String operator, String value) {
        Preconditions.checkNotNull(attribute);
        Preconditions.checkNotNull(operator);
        Preconditions.checkNotNull(value);
        _attribute = attribute;
        _operator = Operator.fromString(operator);
        _value = value;
    }

    /**
     * Construct a constraint from a {@link JSONArray}.
     *
     * The given {@link JSONArray} is assumed to have three strings that
     * represent attribute, operator, and value of a constraint respectively.
     *
     * @param constraint The constraint in {@link JSONArray} representation.
     */
    Constraint(JSONArray constraint) {
        Preconditions.checkNotNull(constraint);
        Preconditions.checkArgument(constraint.length() == 3);
        _attribute = constraint.getString(0);
        _operator = Operator.fromString(constraint.getString(1));
        _value = constraint.getString(2);
    }

    /**
     * @return the attribute of this constraint.
     */
    public String getAttribute() {
        return _attribute;
    }

    /**
     * @return the operator of this constraint.
     */
    public Operator getOperator() {
        return _operator;
    }

    /**
     * @return the value of this constraint.
     */
    public String getValue() {
        return _value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Constraint that = (Constraint) o;

        if (_attribute != null ? !_attribute.equals(that._attribute) : that._attribute != null) return false;
        if (_operator != that._operator) return false;
        return _value != null ? _value.equals(that._value) : that._value == null;
    }

    @Override
    public int hashCode() {
        int result = _attribute != null ? _attribute.hashCode() : 0;
        result = 31 * result + (_operator != null ? _operator.hashCode() : 0);
        result = 31 * result + (_value != null ? _value.hashCode() : 0);
        return result;
    }

    /**
     * @return this constraint as a JSONArray.
     */
    JSONArray toJson() {
        JSONArray jsonArray = new JSONArray();
        jsonArray.put(0, _attribute);
        jsonArray.put(1, _operator.toString());
        jsonArray.put(2, _value);
        return jsonArray;
    }

    // Supported operators

    final public static Operator EQUALS = Operator.EQUALS;

    // Static methods.

    /**
     * Parse a constraint from its {@link JSONArray} representation.
     *
     * @param constraint A constraint expected to parse.
     * @return a parsed constraint.
     */
    public static Constraint parseFrom(JSONArray constraint) {
        return new Constraint(constraint);
    }
}
