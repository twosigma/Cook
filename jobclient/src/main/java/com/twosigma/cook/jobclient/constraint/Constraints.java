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
import com.twosigma.cook.jobclient.constraint.api.Constraint;
import com.twosigma.cook.jobclient.constraint.api.ConstraintBuilder;
import com.twosigma.cook.jobclient.constraint.api.Operator;
import org.json.JSONArray;


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
 *  Constraint c = Constraints.get(Operator.EQUALS).build("host", "foo.bar.com");
 * </pre>
 */
public class Constraints {

    /**
     * Return a {@link ConstraintBuilder} specific for the given operator.
     *
     * @param operator the constraint operator
     * @return a {@link ConstraintBuilder} specific for the given operator.
     */
    public static ConstraintBuilder get(Operator operator) {
        switch (operator) {
            case EQUALS:
                return new OneToOneConstraintBuilder(operator);
            default:
                throw new UnsupportedOperationException(operator + " is not supported.");
        }
    }

    /**
     * @param constraint the {@link JSONArray} representation of a constraint. The {@link JSONArray}
     *                   could have either 2 or 3 elements which represent different forms of constraints.
     *                   See {@link Constraint}.
     * @return the parsed constraint.
     */
    public static Constraint parseFrom(JSONArray constraint) {
        Preconditions.checkArgument(
                constraint.length() == 2 || constraint.length() == 3,
                "The constraint must be form of [<attribute>, <operator>, <value>] or [<attribute>, <operator>]");
        Operator operator = Operator.fromString(constraint.getString(1));
        ConstraintBuilder relation = get(operator);
        return relation.parseFrom(constraint);
    }
}
