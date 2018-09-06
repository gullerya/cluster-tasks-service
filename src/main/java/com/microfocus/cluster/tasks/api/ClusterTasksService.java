/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.api;

import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskStatus;
import com.microfocus.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.concurrent.CompletableFuture;

/**
 * Created by gullery on 08/05/2016.
 * <p>
 * API definition of the service managing Cluster Tasks Dispatchers and cross-functional services (initialization, garbage collector etc)
 */

public interface ClusterTasksService {

	/**
	 * returns instance ID that the current CTS runtime signed with
	 * - instance ID has a runtime retention, its lifespan is the same as the one CTS' main service object (the singleton implementing this interface)
	 * - instance ID is a random UUID, generated anew each time the service object is being created
	 * - instance ID serves internal needs of the library
	 *
	 * @return UUID string
	 */
	String getInstanceID();

	/**
	 * returns promise that is/will be resolved when an initial initialization of the library is performed
	 * - prior to resolution of this promise all tasks related API invocations will result in IllegalStateException
	 * - when this promise is resolved to false all tasks related API invocations will result in IllegalStateException
	 *
	 * @return readiness promise, SHOULD NOT be null
	 */
	CompletableFuture<Boolean> getReadyPromise();

	/**
	 * enqueues tasks for async processing somewhere in the cluster
	 *
	 * @param dataProviderType data provider type which this tasks' processor is working with
	 * @param processorType    target processor type identification
	 * @param tasks            one or more tasks content to be pushed into the queue; MUST NOT be null; MUST NOT be empty
	 * @return an array of enqueue results, corresponding to the array of the tasks, having either the task ID in case of success or an exception in case of failure
	 */
	ClusterTaskPersistenceResult[] enqueueTasks(ClusterTasksDataProviderType dataProviderType, String processorType, ClusterTask... tasks);

	@Deprecated
	int countTasks(ClusterTasksDataProviderType dataProviderType, String processorType, ClusterTaskStatus... statuses);
}