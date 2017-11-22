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

final class TaskConstraint {

    public enum Operator {
        EQUALS("EQUALS");

        private Operator(String name) {
        }

        public Constraint apply(String attribute, String value) {
            return new Constraint(attribute, this, value);
        }

        /**
         * @param op specifies a string representation of operator.
         * @return a constant for the specified name.
         */
        public static TaskConstraint.Operator fromString(String op) {
            return Enum.valueOf(TaskConstraint.Operator.class, op.trim().toUpperCase());
        }
    }

    public static class Constraint {
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
