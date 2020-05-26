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

import com.google.common.base.Preconditions;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * A constraint in Cook could be one of the following three forms
 * <ul>
 * <li>attribute, operator
 * <li>attribute, operator, value
 * <li>attribute, operator, list of values
 * </ul>
 * Examples of constraints are
 * <ul>
 * <li>"host", UNIQUE
 * <li>"host", EQUALS, foo.bar.com
 * <li>"host", IN, [foo1.bar.com,foo2.bar.com]
 * </ul>
 * For now, only EQUALS operator is supported by Cook. One could construct
 * a constraint with an EQUALS operator by
 * <p>
 * <pre>
 *  Constraint c = Constraints.buildEqualsConstraint("host", "foo.bar.com");
 * </pre>
 */
public class Constraints {

    /**
     * @param attribute the constraint attribute
     * @param value     the constraint value
     * @return an EQUALS constraint with given attribute and value.
     */
    public static Constraint buildEqualsConstraint(String attribute, String value) {
        return new OneToOneConstraint(Operator.EQUALS, attribute, value);
    }

    /**
     * @param constraint the {@link JSONArray} representation of a constraint. The {@link JSONArray}
     *                   could have either 2 or 3 elements which represent different forms of constraints.
     *                   See {@link Constraint}.
     * @return the parsed constraint.
     */
    public static Constraint parseFrom(JSONArray constraint)
        throws JSONException {
        Operator operator = Operator.fromString(constraint.getString(1));
        switch (operator) {
            case EQUALS:
                Preconditions.checkArgument(constraint.length() == 3,
                        "The constraint must be form of [<attribute>, <operator>, <value>]");
                return new OneToOneConstraint(operator, constraint.getString(0), constraint.getString(2));
            default:
                throw new UnsupportedOperationException(operator + " is not supported.");
        }
    }
}
