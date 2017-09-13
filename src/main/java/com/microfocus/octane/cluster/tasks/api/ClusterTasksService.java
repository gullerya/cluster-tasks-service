package com.microfocus.octane.cluster.tasks.api;

/**
 * Created by gullery on 08/05/2016.
 * <p>
 * API definition of the service managing Cluster Tasks Dispatchers and cross-functional services (initialization, garbage collector etc)
 */

public interface ClusterTasksService {

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
