/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.api.builders;

import com.microfocus.cluster.tasks.api.dto.ClusterTask;

/**
 * Base API for a lowest common denominating Task Builder
 */

public interface TaskBuilder {

	/**
	 * Sets delay for the task execution
	 * - delay will be counted from the point of tasks enqueue
	 * - delay promised to occur in N to (N + tasks dispatch interval), which normally is ~1 second
	 * - if not explicitly set, delay is taken to be 0 millis
	 *
	 * @param delayByMillis delay for task execution, in milliseconds
	 * @return task builder instance
	 * @throws IllegalStateException if the {@link #build() build} method has already been called on this builder instance
	 */
	TaskBuilder setDelayByMillis(Long delayByMillis) throws IllegalStateException;

	/**
	 * Sets task's body
	 * - body can be in any size and content layout, is it a responsibility of the consumer to serialize/deserialize the raw string
	 * - if not explicitly set, body is taken to be NULL
	 *
	 * @param body task's body
	 * @return task builder instance
	 * @throws IllegalStateException if the {@link #build() build} method has already been called on this builder instance
	 */
	TaskBuilder setBody(String body) throws IllegalStateException;

	/**
	 * Finalizes the build task process and locks the builder instance for further changes
	 *
	 * @return built task
	 * @throws IllegalStateException if the this method has already been called on this builder instance
	 */
	ClusterTask build() throws IllegalStateException;
}
