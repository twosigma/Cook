package com.twosigma.cook.jobclient;

import com.google.common.base.Preconditions;
import org.json.JSONArray;
import org.json.JSONObject;

final class TaskConstraint {

    public enum Operator {
        EQUALS("EQUALS");

        private Operator(String name) {
        }

        /**
         * @param name specifies a string representation of status.
         * @return a constant for the specified name.
         */
        public static TaskConstraint.Operator fromString(String name) {
            return Enum.valueOf(TaskConstraint.Operator.class, name.trim().toUpperCase());
        }
    }

    static class Constraint {
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

        String getAttribute() {
            return _attribute;
        }

        Operator getOperator() {
            return _operator;
        }

        String getValue() {
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
    }
}
