package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorDefault;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gullery on 10/05/2016.
 * <p>
 * Base implementation of Cluster tasks workers Factory
 */

class ClusterTaskWorkersFactory {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTaskWorkersFactory.class);

	ClusterTasksWorker createWorker(ClusterTasksDataProvider dataProvider, ClusterTasksProcessorDefault processor, ClusterTask task) {
		if (processor == null) {
			throw new IllegalArgumentException("processor MUST NOT be null");
		}
		if (task == null) {
			throw new IllegalArgumentException("task MUST NOT be null");
		}

		return new ClusterTasksWorkerImpl(dataProvider, processor, task);
	}

	private class ClusterTasksWorkerImpl implements ClusterTasksWorker {
		private final ClusterTasksDataProvider dataProvider;
		private final ClusterTasksProcessorDefault tasksProcessor;
		private final ClusterTask task;

		private ClusterTasksWorkerImpl(ClusterTasksDataProvider dataProvider, ClusterTasksProcessorDefault tasksProcessor, ClusterTask task) {
			this.dataProvider = dataProvider;
			this.tasksProcessor = tasksProcessor;
			this.task = task;
		}

		@Override
		public void run() {
			if (task.getPartitionIndex() != null) {
				try {
					task.setBody(dataProvider.retrieveTaskBody(task.getId(), task.getPartitionIndex()));
					logger.debug(task + " has body: " + task.getBody());
				} catch (Exception e) {
					logger.error("failed to retrieve body of the " + task + ", aborting task's execution");
					return;
				}
			} else {
				logger.debug(task + " is bodiless");
			}

			try {
				tasksProcessor.processTask(task);
			} catch (Exception e) {
				logger.error("failed processing " + task + ", body: " + task.getBody(), e);
			} finally {
				try {
					if (task.getTaskType() == ClusterTaskType.REGULAR) {
						dataProvider.updateTaskFinished(task.getId());
					} else if (task.getTaskType() == ClusterTaskType.SCHEDULED) {
						dataProvider.updateTaskReenqueued(task.getId());
					}
				} catch (Exception e) {
					logger.error("failed to update finished on " + task, e);
				} finally {
					tasksProcessor.notifyTaskWorkerFinished();
				}
			}
		}
	}
}
