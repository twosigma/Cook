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
package com.twosigma.cook.jobclient.constraint;

import org.json.JSONArray;

/**
 * A constraint of form
 * <ul>
 * attribute, operator, value
 * </ul>
 */
final class OneToOneConstraint implements Constraint {
    private Operator _operator;
    private String _attribute;
    private String _value;

    OneToOneConstraint(Operator operator, String attribute, String value) {
        _operator = operator;
        _attribute = attribute;
        _value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OneToOneConstraint that = (OneToOneConstraint) o;

        if (_attribute != null ? !_attribute.equals(that._attribute) : that._attribute != null) {
            return false;
        }
        if (_operator != that._operator) {
            return false;
        }
        return _value != null ? _value.equals(that._value) : that._value == null;
    }

    @Override
    public int hashCode() {
        int result = _attribute != null ? _attribute.hashCode() : 0;
        result = 31 * result + (_operator != null ? _operator.hashCode() : 0);
        result = 31 * result + (_value != null ? _value.hashCode() : 0);
        return result;
    }

    @Override
    public JSONArray toJson() {
        JSONArray jsonArray = new JSONArray();
        jsonArray.put(0, _attribute);
        jsonArray.put(1, _operator.toString());
        jsonArray.put(2, _value);
        return jsonArray;
    }

    @Override
    public String getAttribute() {
        return _attribute;
    }

    @Override
    public Operator getOperator() {
        return _operator;
    }
}
