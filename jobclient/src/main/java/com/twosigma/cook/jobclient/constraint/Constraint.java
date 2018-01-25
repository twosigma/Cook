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

/**
 * The interface to specify a constraint in Cook
 * <p>
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
 */
public interface Constraint {
    /**
     * @return this constraint as a JSONArray.
     */
    JSONArray toJson() throws JSONException;

    /**
     * @return the attribute of this constraint.
     */
    String getAttribute();

    /**
     * @return the operator of this constraint.
     */
    Operator getOperator();
}
