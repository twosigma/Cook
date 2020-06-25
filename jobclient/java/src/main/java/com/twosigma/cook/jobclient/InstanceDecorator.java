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

/**
 * The interface of instance decorator which will take an instance builder as input and return a decorated instance
 * builder.
 *
 * Created: June 23, 2016
 * @author wzhao
 */
public interface InstanceDecorator {

    /**
     * @param builder The {@link Instance.Builder} expected to decorate.
     * @return a decorated instance builder.
     */
    Instance.Builder decorate(Instance.Builder builder);
}
