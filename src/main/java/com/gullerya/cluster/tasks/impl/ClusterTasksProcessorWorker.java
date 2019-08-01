package com.gullerya.cluster.tasks.impl;

import com.gullerya.cluster.tasks.api.enums.ClusterTaskType;
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
	private final ClusterTaskImpl task;

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

	ClusterTasksProcessorWorker(ClusterTasksDataProvider dataProvider, ClusterTasksProcessorBase processor, ClusterTaskImpl task) {
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
		//  reinsert scheduled task at the soonest possible point in time
		if (task.taskType == ClusterTaskType.SCHEDULED) {
			reinsertScheduledTask(task);
		}

		Summary.Timer taskSelfDurationTimer = tasksPerProcessorDuration.labels(processor.getType()).startTimer();           //  metric
		try {
			if (enrichTaskWithBodyIfRelevant(task)) {
				ClusterTaskImpl clusterTask = new ClusterTaskImpl(task);
				String weakHash = CTSUtils.get6CharsChecksum(processor.getType());
				if (clusterTask.concurrencyKey != null && clusterTask.concurrencyKey.endsWith(weakHash)) {
					clusterTask.concurrencyKey = clusterTask.concurrencyKey.substring(0, clusterTask.concurrencyKey.length() - 6);
				}
				processor.processTask(clusterTask);
			} else {
				logger.error(task + " found to have body, but body retrieval failed (see previous logs), won't execute");
			}
		} catch (Throwable t) {
			logger.error("failed processing " + task + ", body: " + (task.body == null
					? null
					: task.body.substring(0, Math.min(5000, task.body.length()))
			), t);
			errorsPerProcessorCounter.labels(processor.getType(), t.getClass().getSimpleName()).inc();                      //  metric
		} finally {
			taskSelfDurationTimer.observeDuration();                                                                        //  metric
			try {
				removeFinishedTask(task.id);
			} catch (Throwable t) {
				logger.error("failed to remove finished " + task, t);
				ctsOwnErrorsCounter.labels(TASK_FINALIZATION_PHASE, t.getClass().getSimpleName()).inc();                    //  metric
			} finally {
				processor.notifyTaskWorkerFinished(dataProvider, task);
			}
		}
	}

	//  scheduled task reinsert is mission critical part of functionality - MUST be handled and validated
	private void reinsertScheduledTask(ClusterTaskImpl originalTask) {
		ClusterTaskImpl newTask = new ClusterTaskImpl(originalTask);
		boolean reinserted = CTSUtils.retry(6, () -> {
			int reinsertResult = dataProvider.reinsertScheduledTasks(Collections.singletonList(newTask));
			if (reinsertResult == 1) {
				return true;
			} else {
				logger.warn("unexpectedly failed to reschedule self (reinsert result is " + reinsertResult + ")");
				return false;
			}
		});
		if (!reinserted) {
			logger.error("finally failed to reinsert schedule task " + processor.getType());
		}
	}

	private boolean enrichTaskWithBodyIfRelevant(ClusterTaskImpl task) {
		if (task.partitionIndex != null) {
			return CTSUtils.retry(3, () -> {
				try {
					task.body = dataProvider.retrieveTaskBody(task.id, task.partitionIndex);
					if (logger.isDebugEnabled()) {
						logger.debug(task + " has body: " + (task.body == null
								? null
								: task.body.substring(0, Math.min(5000, task.body.length()))
						));
					}
					return true;
				} catch (Throwable t) {
					ctsOwnErrorsCounter.labels(BODY_RETRIEVAL_PHASE, t.getClass().getSimpleName()).inc();                   //  metric
					return false;
				}
			});
		} else {
			logger.debug(task + " is bodiless");
			return true;
		}
	}

	//  task removal is mission critical part of functionality - MUST be handled and validated
	private void removeFinishedTask(Long taskId) {
		boolean done = CTSUtils.retry(12, () -> dataProvider.removeTaskById(taskId));

		if (!done) {
			logger.error("possibly CRITICAL error, failed to remove task " + taskId + ", check its uniqueness/concurrency settings and remove manually if needed");
		}
	}
}
