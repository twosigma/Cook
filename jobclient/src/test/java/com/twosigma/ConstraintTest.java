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
package com.twosigma;

import com.twosigma.cook.jobclient.constraint.Constraints;
import com.twosigma.cook.jobclient.constraint.api.Constraint;
import com.twosigma.cook.jobclient.constraint.api.Operator;
import org.junit.Assert;
import org.junit.Test;

public class ConstraintTest {

    @Test
    public void testScope() {
        Constraint c = Constraints.get(Operator.EQUALS).build("bar", "foo");
        Assert.assertEquals(c.getAttribute(), "bar");
        Assert.assertEquals(c.getOperator(), Operator.EQUALS);
        Assert.assertEquals(c.getValue(), "foo");
    }
}
