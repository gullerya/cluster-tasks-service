/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.api.builders;

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
	 * @param uniquenessKey uniqueness key (40 chars max length)
	 * @return task builder instance
	 * @throws IllegalStateException    if the {@link com.microfocus.cluster.tasks.api.builders.TaskBuilder#build() build} method has already been called on this builder instance
	 * @throws IllegalArgumentException if the key is NULL or EMPTY of bigger than allowed
	 */
	TaskBuilder setUniquenessKey(String uniquenessKey) throws IllegalStateException, IllegalArgumentException;
}
