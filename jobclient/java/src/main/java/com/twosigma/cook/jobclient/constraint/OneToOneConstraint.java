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
import org.json.JSONException;

import java.util.Objects;

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
        _attribute = attribute.trim();
        _value = value.trim();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (o == this) return true;

        if (!(o instanceof OneToOneConstraint)) return false;

        OneToOneConstraint other = (OneToOneConstraint) o;

        if (!Objects.equals(this._operator, other._operator)) return false;
        if (!Objects.equals(this._attribute, other._attribute)) return false;
        if (!Objects.equals(this._value, other._value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(_operator, _attribute, _value);
    }

    @Override
    public JSONArray toJson()
        throws JSONException {
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
