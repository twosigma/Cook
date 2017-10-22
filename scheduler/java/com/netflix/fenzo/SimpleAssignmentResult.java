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

package com.netflix.fenzo;

import java.util.List;
import java.util.Map;


public class SimpleAssignmentResult extends TaskAssignmentResult {

    public SimpleAssignmentResult(List<AssignmentFailure> assignmentFailures,
                                  ConstraintFailure constraintFailure) {
        this(assignmentFailures, constraintFailure, getDummyRequest());
    }

    public SimpleAssignmentResult(List<AssignmentFailure> assignmentFailures,
                                  ConstraintFailure constraintFailure,
                                  TaskRequest request) {
        super(SimpleAssignmentResult.getDummyAvm(),
              request,
              false,
              assignmentFailures,
              constraintFailure,
              0.0);
    }

    public static AssignableVirtualMachine getDummyAvm() {
        return new AssignableVirtualMachine(null, null, "my.fake.host",
                                            null, 10, null);
    }

    public static TaskRequest getDummyRequest() {
        return new TaskRequest() {
            @Override
            public String getId() {
                return "fake-task-id";
            }

            @Override
            public String taskGroupName() {
                return "fake-group-name";
            }

            @Override
            public double getCPUs() {
                return 2.0;
            }
            @Override
            public double getMemory() {
                return 256.0;
            }
            @Override
            public double getNetworkMbps() {
                return 128.0;
            }
            @Override
            public double getDisk() {
                return 4096.0;
            }
            @Override
            public int getPorts() {
                return 1;
            }
            @Override
            public Map<String, Double> getScalarRequests() {
                return null;
            }
            @Override
            public List<? extends ConstraintEvaluator> getHardConstraints() {
                return null;
            }
            @Override
            public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
                return null;
            }
            @Override
            public void setAssignedResources(AssignedResources assignedResources) {
            }
            @Override
            public AssignedResources getAssignedResources() {
                return null;
            }
            @Override
            public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
                return null;
            }
        };
   }

}
