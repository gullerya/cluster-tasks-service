/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.api.builders;

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
	 * - REQUIRED to have defined concurrency key (40 chars maximum)
	 * - ONLY a SINGLE task of a specific concurrency key will be executed at any given moment ACROSS the cluster
	 * - concurrency/channelling is enforced ACROSS processors as well
	 * - having the rest of the attributes of the simple task
	 *
	 * @return Channel tasks' oriented TaskBuilder
	 */
	public static ChanneledTaskBuilder channeledTask() {
		return new ChanneledTaskBuilderImpl();
	}

	/**
	 * Entry point for creation of UNIQUE task, which is:
	 * - REQUIRED to have defined uniqueness key (40 chars maximum)
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
}
