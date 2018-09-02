/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskType;
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

class ClusterTasksWorker implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksWorker.class);
	private static final Counter ctsOwnErrorsCounter;
	private static final Counter tasksPerProcessorCounter;
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
		tasksPerProcessorCounter = Counter.build()
				.name("cts_per_processor_tasks_total")
				.help("CTS task counter (per processor type)")
				.labelNames("processor_type")
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

	ClusterTasksWorker(ClusterTasksDataProvider dataProvider, ClusterTasksProcessorBase processor, TaskInternal task) {
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
		tasksPerProcessorCounter.labels(processor.getType()).inc();                                         //  metric
		if (task.partitionIndex != null) {
			try {
				task.body = dataProvider.retrieveTaskBody(task.id, task.partitionIndex);
				logger.debug(task + " has body: " + task.body);
			} catch (Exception e) {
				logger.error("failed to retrieve body of the " + task + ", aborting task's execution");
				ctsOwnErrorsCounter.labels(BODY_RETRIEVAL_PHASE, e.getClass().getSimpleName()).inc();      //  metric
				return;
			}
		} else {
			logger.debug(task + " is bodiless");
		}

		Summary.Timer timer = tasksPerProcessorDuration.labels(processor.getType()).startTimer();           //  metric
		try {
			processor.processTask(ClusterTaskImpl.from(task));
		} catch (Throwable e) {
			logger.error("failed processing " + task + ", body: " + task.body, e);
			errorsPerProcessorCounter.labels(processor.getType(), e.getClass().getSimpleName()).inc();      //  metric
		} finally {
			timer.observeDuration();                                                                        //  metric
			try {
				if (task.taskType == ClusterTaskType.SCHEDULED) {
					dataProvider.reinsertScheduledTasks(Collections.singletonList(task));
				}
				dataProvider.updateTaskToFinished(task.id);
			} catch (Exception e) {
				logger.error("failed to update finished on " + task, e);
				ctsOwnErrorsCounter.labels(TASK_FINALIZATION_PHASE, e.getClass().getSimpleName()).inc();   //  metric
			} finally {
				processor.notifyTaskWorkerFinished();
			}
		}
	}
}
