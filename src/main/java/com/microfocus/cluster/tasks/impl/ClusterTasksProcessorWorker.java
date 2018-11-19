/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.impl;

import com.microfocus.cluster.tasks.api.enums.ClusterTaskType;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Created by gullery on 10/05/2016.
 * <p>
 * Cluster task worker API: wrapper of the actual task handling business logic, meant to be used internally by CTS
 * New instance of this class is created for each task
 * Prometheus counters are static as they are once and for all
 */

class ClusterTasksProcessorWorker implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksProcessorWorker.class);
	private static final Counter ctsOwnErrorsCounter;
	private static final Summary tasksPerProcessorDuration;
	private static final Counter errorsPerProcessorCounter;
	private static final String BODY_RETRIEVAL_PHASE = "body_retrieve";
	private static final String TASK_FINALIZATION_PHASE = "task_finalization";

	private final ClusterTasksDataProvider dataProvider;
	private final ClusterTasksProcessorBase processor;
	private final TaskInternal task;

	static {
		ctsOwnErrorsCounter = Counter.build()
				.name("cts_own_errors_total")
				.help("CTS own errors counter")
				.labelNames("phase", "error_type")
				.register();
		tasksPerProcessorDuration = Summary.build()
				.name("cts_per_processor_task_duration_seconds")
				.help("CTS task duration summary (per processor type)")
				.labelNames("processor_type")
				.register();
		errorsPerProcessorCounter = Counter.build()
				.name("cts_per_processor_errors_total")
				.help("Tasks errors caught by CTS (per processor type)")
				.labelNames("processor_type", "error_type")
				.register();
	}

	ClusterTasksProcessorWorker(ClusterTasksDataProvider dataProvider, ClusterTasksProcessorBase processor, TaskInternal task) {
		if (processor == null) {
			throw new IllegalArgumentException("processor MUST NOT be null");
		}
		if (task == null) {
			throw new IllegalArgumentException("task MUST NOT be null");
		}
		this.dataProvider = dataProvider;
		this.processor = processor;
		this.task = task;
	}

	@Override
	public void run() {
		if (task.partitionIndex != null) {
			try {
				task.body = dataProvider.retrieveTaskBody(task.id, task.partitionIndex);
				logger.debug(task + " has body: " + task.body);
			} catch (Throwable t) {
				logger.error("failed to retrieve body of the " + task + ", aborting task's execution", t);
				ctsOwnErrorsCounter.labels(BODY_RETRIEVAL_PHASE, t.getClass().getSimpleName()).inc();                   //  metric
				return;
			}
		} else {
			logger.debug(task + " is bodiless");
		}

		Summary.Timer taskSelfDurationTimer = tasksPerProcessorDuration.labels(processor.getType()).startTimer();       //  metric
		try {
			processor.processTask(ClusterTaskImpl.from(task));
		} catch (Throwable t) {
			logger.error("failed processing " + task + ", body: " + task.body, t);
			errorsPerProcessorCounter.labels(processor.getType(), t.getClass().getSimpleName()).inc();                  //  metric
		} finally {
			taskSelfDurationTimer.observeDuration();                                                                    //  metric
			try {
				long taskIdToRemove = task.id;
				if (task.taskType == ClusterTaskType.SCHEDULED) {
					int reinsertResult = dataProvider.reinsertScheduledTasks(Collections.singletonList(task));
					if (reinsertResult != 1) {
						logger.warn("unexpectedly failed to reschedule self (reinsert result is " + reinsertResult + ")");
					}
				}
				removeFinishedTask(taskIdToRemove);
			} catch (Throwable t) {
				logger.error("failed to update finished on " + task, t);
				ctsOwnErrorsCounter.labels(TASK_FINALIZATION_PHASE, t.getClass().getSimpleName()).inc();                //  metric
			} finally {
				processor.notifyTaskWorkerFinished(dataProvider, task);
			}
		}
	}

	//  task removal is mission critical part of functionality - MUST be handled and validated
	private void removeFinishedTask(Long taskId) {
		boolean done = false;
		int attempts = 0;
		int maxAttempts = 12;
		do {
			attempts++;
			try {
				done = dataProvider.removeTaskById(taskId);
			} catch (Exception e) {
				logger.error("failed to remove task " + taskId + ", attempts " + attempts + " out of max " + maxAttempts, e);
			}
		} while (!done && attempts < maxAttempts);
		if (!done) {
			logger.error("possibly CRITICAL error, failed to remove task " + taskId + ", check its uniqueness/concurrency settings and remove manually if needed");
		}
	}
}
