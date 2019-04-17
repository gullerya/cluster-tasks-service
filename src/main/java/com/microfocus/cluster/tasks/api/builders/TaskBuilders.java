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
 * Tasks builder is the only valid mean to create guaranteed valid tasks to submit to Cluster Tasks Service
 */
public class TaskBuilders {

	private TaskBuilders() {
	}

	/**
	 * Entry point for creation of SIMPLE task, which is:
	 * - NOT having any uniqueness and/or concurrency constrains
	 * - ABLE to have a body of arbitrary size
	 * - typically will be executed simply in chronological order of submission, UNLESS...
	 * - possibly provided with ordering/delaying properties, which will affect the execution order
	 *
	 * @return TaskBuilder providing an ability to define any relevant property of the SIMPLE task or its derivative
	 */
	public static TaskBuilder simpleTask() {
		return new SimpleTaskBuilder();
	}

	/**
	 * Entry point for creation of CHANNELED task, which is:
	 * - REQUIRED to have defined concurrency key (34 chars maximum)
	 * - ONLY a SINGLE task of a specific concurrency key will be executed at any given moment ACROSS the cluster
	 * - concurrency/channelling is enforced PER processor (2 tasks submitted for 2 different processor with the same concurrency key will [possibly] run in parallel)
	 * - having the rest of the attributes of the simple task
	 *
	 * @return Channel tasks' oriented TaskBuilder
	 */
	public static ChanneledTaskBuilder channeledTask() {
		return new ChanneledTaskBuilderImpl();
	}

	/**
	 * Entry point for creation of UNIQUE task, which is:
	 * - REQUIRED to have defined uniqueness key (34 chars maximum)
	 * - WON'T have concurrency notion (obviously, there is nothing to concur with)
	 * - ONLY a single task of a specific uniqueness key will be present at any given moment in the queue, UNLESS...
	 * - there is another task of the same uniqueness key already RUNNING (thus enabling continuous existence of some unique task in the system without a danger to cut the chain)
	 * - having the rest of the attributes of the simple task
	 *
	 * @return Unique tasks' oriented TaskBuilder
	 */
	public static UniqueTaskBuilder uniqueTask() {
		return new UniqueTaskBuilderImpl();
	}

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

	/**
	 * API for CHANNELED task builder
	 * - channeled tasks should be used when there is a need to process tasks in a strict serial order
	 * - regardless of available resources (treads) across the cluster, only a single task per concurrency key will be running in the whole cluster
	 * - tuning the number of resources (treads) per processor from one side, and a well-thought concurrency key from the other - those two provide
	 * good means to tune CTS throughput and resources utilization
	 */
	public interface ChanneledTaskBuilder {

		/**
		 * Sets concurrency key for the task
		 * - concurrency key will cause all tasks having the same key to be executed SERIALLY
		 * - it is promised, that at any given moment only one (or zero) task of a specific concurrency key will be executed across all the treads across all the nodes in the cluster
		 * - concurrency key effective across the processors
		 *
		 * @param concurrencyKey concurrency key (34 chars max length)
		 * @return task builder instance
		 * @throws IllegalStateException    if the {@link com.microfocus.cluster.tasks.api.builders.TaskBuilders.TaskBuilder#build() build} method has already been called on this builder instance
		 * @throws IllegalArgumentException if the key is NULL or EMPTY of bigger than allowed
		 */
		TaskBuilders.TaskBuilder setConcurrencyKey(String concurrencyKey) throws IllegalStateException, IllegalArgumentException;
	}

	/**
	 * API for UNIQUE task builder
	 * - unique tasks is mostly meant for the cases, when few separate processes may independently come to decision to execute the same task
	 * CTS's unique task mechanism will allow to synchronize it across the cluster in such a way, that only one task will be created
	 * - once a unique task dispatched, new task may come along with the same uniqueness key
	 * - ability to enqueue new unique task while one of the same uniqueness key is already running enables to create effect of a single recurring task,
	 * while there is no danger of accidentally breaking the chain from one side, and multiplying the tasks from another
	 * - if the desired flow is to have a single unique task running each specific (yet even adjustable) period of time - scheduled tasks mechanism should be checked
	 */
	public interface UniqueTaskBuilder {

		/**
		 * Sets uniqueness key
		 * - only 1 (or zero) non-running task of the specific uniqueness key may be present in the queue
		 * - if there already is pending task in the queue with the same uniqueness key, the task will be rejected upon enqueueing (see possible results of enqueue tasks method)
		 * - if there is a running task of the same uniqueness key - the new task is allowed
		 * - uniqueness key is enforced only in context of the specific tasks processor, in other words 2 different processors may have co-existing tasks with the same uniqueness key
		 *
		 * @param uniquenessKey uniqueness key (34 chars max length)
		 * @return task builder instance
		 * @throws IllegalStateException    if the {@link com.microfocus.cluster.tasks.api.builders.TaskBuilders.TaskBuilder#build() build} method has already been called on this builder instance
		 * @throws IllegalArgumentException if the key is NULL or EMPTY of bigger than allowed
		 */
		TaskBuilder setUniquenessKey(String uniquenessKey) throws IllegalStateException, IllegalArgumentException;
	}
}
