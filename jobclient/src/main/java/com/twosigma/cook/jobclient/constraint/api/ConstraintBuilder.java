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
package com.twosigma.cook.jobclient.constraint.api;

import org.json.JSONArray;

public interface ConstraintBuilder {
    /**
     * @return the built-in operator for this constraint builder.
     */
    Operator getOperator();

    /**
     * Build a constraint from the given attribute and value(s).
     *
     * @param attribute the constraint attribute
     * @param values    the constraint values(s)
     * @return a constraint with built-in operator together with provided attribute and value(s).
     */
    Constraint build(String attribute, Object... values);

    /**
     * @param constraint the {@link JSONArray} representation of a constraint. The {@link JSONArray}
     *                   could have either 2 or 3 elements which represent different forms of constraints.
     *                   See {@link Constraint}.
     * @return the parsed constraint.
     */
    Constraint parseFrom(JSONArray constraint);
}
