package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gullery on 10/05/2016.
 * <p>
 * Base implementation of Cluster tasks workers Factory
 */

class ClusterTaskWorkersFactory {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTaskWorkersFactory.class);

	ClusterTasksWorker createWorker(ClusterTasksDataProvider dataProvider, ClusterTasksProcessorBase processor, TaskInternal task) {
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
		private final ClusterTasksProcessorBase tasksProcessor;
		private final TaskInternal task;

		private ClusterTasksWorkerImpl(ClusterTasksDataProvider dataProvider, ClusterTasksProcessorBase tasksProcessor, TaskInternal task) {
			this.dataProvider = dataProvider;
			this.tasksProcessor = tasksProcessor;
			this.task = task;
		}

		@Override
		public void run() {
			if (task.partitionIndex != null) {
				try {
					task.body = dataProvider.retrieveTaskBody(task.id, task.partitionIndex);
					logger.debug(task + " has body: " + task.body);
				} catch (Exception e) {
					logger.error("failed to retrieve body of the " + task + ", aborting task's execution");
					return;
				}
			} else {
				logger.debug(task + " is bodiless");
			}

			try {
				tasksProcessor.processTask(TaskToProcessImpl.from(task));
			} catch (Exception e) {
				logger.error("failed processing " + task + ", body: " + task.body, e);
			} finally {
				try {
					if (task.taskType == ClusterTaskType.REGULAR) {
						dataProvider.updateTaskToFinished(task.id);
					} else if (task.taskType == ClusterTaskType.SCHEDULED) {
						dataProvider.updateTaskToReenqueued(task.id);
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
