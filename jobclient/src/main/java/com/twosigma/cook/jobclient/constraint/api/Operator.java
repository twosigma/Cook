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