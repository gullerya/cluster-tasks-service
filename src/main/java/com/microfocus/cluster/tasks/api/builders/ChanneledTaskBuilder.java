/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.api.builders;

/**
 * API for CHANNELED task builder
 * - channeled tasks should be used when there is a need to process tasks in a strict serial order
 * - regardless of available resources (treads) across the cluster, only a single task per concurrency key will be running in the whole cluster
 * - tuning the number of resources (treads) per processor from one side, and a well-thought concurrency key from the other - those two provide
 * good means to tune CTS throughput and resources utilization
 * - when channeled task specific part is accomplished, falls back to the common task builder
 */

public interface ChanneledTaskBuilder {

	/**
	 * Sets concurrency key for the task
	 * - concurrency key will for all tasks having the same key to be executed SERIALLY
	 * - it is promised, that at any given moment only one (or zero) task of a specific concurrency key will be executed across all the treads across all the nodes in the cluster
	 * - concurrency key effective across the processors
	 *
	 * @param concurrencyKey concurrency key (40 chars max length)
	 * @return task builder instance
	 * @throws IllegalStateException    if the {@link com.microfocus.cluster.tasks.api.builders.TaskBuilder#build() build} method has already been called on this builder instance
	 * @throws IllegalArgumentException if the key is NULL or EMPTY of bigger than allowed
	 */
	TaskBuilder setConcurrencyKey(String concurrencyKey) throws IllegalStateException, IllegalArgumentException;
}
