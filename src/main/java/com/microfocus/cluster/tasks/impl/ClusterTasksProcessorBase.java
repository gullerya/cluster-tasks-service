/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.impl;

import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import io.prometheus.client.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by gullery on 08/05/2016.
 * <p>
 * Base class for every ClusterTasksProcessor to derive from, used for an internal inter-op between the CTService and all of the CTProcessors
 */

public abstract class ClusterTasksProcessorBase {
	private final Logger logger = LoggerFactory.getLogger(ClusterTasksProcessorBase.class);
	private static final String NON_CONCURRENT_TASKS_GROUP_KEY = "NULL";
	private static final Gauge threadsUtilizationGauge;

	private final String type;
	private final ClusterTasksDataProviderType dataProviderType;
	private final AtomicInteger availableWorkers = new AtomicInteger(0);
	private final Map<String, Long> concurrencyKeysFairnessMap = new LinkedHashMap<>();
	private int numberOfWorkersPerNode;
	private int minimalTasksTakeInterval;
	private long lastTaskHandledLocalTime;
	private ExecutorService workersThreadPool;

	@Autowired
	private ClusterTasksServiceImpl clusterTasksService;

	static {
		threadsUtilizationGauge = Gauge.build()
				.name("cts_per_processor_threads_utilization_percents")
				.help("CTS per-processor threads utilization")
				.labelNames("processor_type")
				.register();
	}

	protected ClusterTasksProcessorBase(ClusterTasksDataProviderType dataProviderType, int numberOfWorkersPerNode) {
		this(dataProviderType, numberOfWorkersPerNode, 0);
	}

	protected ClusterTasksProcessorBase(ClusterTasksDataProviderType dataProviderType, int numberOfWorkersPerNode, int minimalTasksTakeInterval) {
		this.type = this.getClass().getSimpleName();
		this.dataProviderType = dataProviderType;
		this.numberOfWorkersPerNode = numberOfWorkersPerNode;
		this.minimalTasksTakeInterval = minimalTasksTakeInterval;
	}

	@PostConstruct
	private void initialize() {
		workersThreadPool = Executors.newFixedThreadPool(numberOfWorkersPerNode, new CTPWorkersThreadFactory());
		availableWorkers.set(numberOfWorkersPerNode);

		logger.info(this.type + " initialized: data provider type: " + dataProviderType + "; worker threads per node: " + numberOfWorkersPerNode);
	}

	//
	//  EXPOSED API / EXTENSIBILITY POINTS
	//

	/**
	 * returns processor's type key
	 * - MUST be a NON-NULL and NON-EMPTY string
	 * - any 2 different processors is the single process MUST NOT have the same type (it is possible to run different processors for the same type in different processes, FWIW)
	 * - as a default, processor's simple class name is taken, but MAY be overrode by custom logic
	 *
	 * @return processor's type (key)
	 */
	protected String getType() {
		return type;
	}

	/**
	 * sets a minimal interval that the processor is ready to take new tasks from the service
	 * - this is the lower level of processors throughput / resources utilization (along with configuration of threads per node)
	 * - delayed tasks MAY are expected to run withing the following span of time: delay - delay + tasksTakeInterval
	 *
	 * @param minimalTasksTakeInterval interval of the processor's readiness to take tasks, in milliseconds
	 */
	protected void setMinimalTasksTakeInterval(int minimalTasksTakeInterval) {
		this.minimalTasksTakeInterval = Math.max(minimalTasksTakeInterval, 0);
	}

	/**
	 * gets a processor's status as of ability to handle another task
	 * - delayed tasks MAY are expected to run withing the following span of time: delay - delay + tasksTakeInterval
	 *
	 * @return current condition of the processor as of readiness to take [any] task
	 */
	protected boolean isReadyToHandleTask() {
		return true;
	}

	/**
	 * processor's custom task processing logic
	 *
	 * @param task task that is to be processed
	 * @throws Exception processor MAY throw Exception and the service will manage it (catch, log, metrics)
	 */
	abstract public void processTask(ClusterTask task) throws Exception;

	//
	//  INTERNAL STUFF FROM HERE
	//
	final ClusterTasksDataProviderType getDataProviderType() {
		return dataProviderType;
	}

	final boolean isReadyToHandleTaskInternal() {
		long FOREIGN_CHECK_DURATION_THRESHOLD = 5;
		boolean internalResult = true;
		if (availableWorkers.get() == 0) {
			internalResult = false;
		} else if (minimalTasksTakeInterval > 0) {
			internalResult = System.currentTimeMillis() - lastTaskHandledLocalTime > minimalTasksTakeInterval;
		}

		boolean foreignResult = true;
		if (internalResult) {
			long foreignCallStart = System.currentTimeMillis();
			foreignResult = isReadyToHandleTask();
			long foreignCallDuration = System.currentTimeMillis() - foreignCallStart;
			if (foreignCallDuration > FOREIGN_CHECK_DURATION_THRESHOLD) {
				logger.warn("call to a foreign method 'isReadyToHandleTask' took more than " + FOREIGN_CHECK_DURATION_THRESHOLD + "ms (" + foreignCallDuration + "ms)");
			}
		}
		return internalResult && foreignResult;
	}

	final Collection<TaskInternal> selectTasksToRun(List<TaskInternal> candidates) {
		Map<Long, TaskInternal> tasksToRunByID = new LinkedHashMap<>();
		int availableWorkersTmp = availableWorkers.get();

		candidates.sort(Comparator.comparing(t -> t.orderingFactor));

		//  group tasks by concurrency key
		Map<String, List<TaskInternal>> tasksGroupedByConcurrencyKeys = candidates.stream()
				.collect(Collectors.groupingBy(t -> t.concurrencyKey != null ? t.concurrencyKey : NON_CONCURRENT_TASKS_GROUP_KEY));

		//  order relevant concurrency keys by fairness logic
		List<String> orderedRelevantKeys = new ArrayList<>(tasksGroupedByConcurrencyKeys.keySet());
		orderedRelevantKeys.sort((keyA, keyB) -> {
			Long keyALastTouch = concurrencyKeysFairnessMap.getOrDefault(keyA, 0L);
			Long keyBLastTouch = concurrencyKeysFairnessMap.getOrDefault(keyB, 0L);
			return Long.compare(keyALastTouch, keyBLastTouch);
		});

		//  first - select tasks fairly - including NON_CONCURRENT_TASKS_GROUP to let them chance to run as well
		//  here we taking a single (first) task from each CONCURRENT CHANNEL
		for (String concurrencyKey : orderedRelevantKeys) {
			if (availableWorkersTmp <= 0) break;
			List<TaskInternal> channeledTasksGroup = tasksGroupedByConcurrencyKeys.get(concurrencyKey);
			tasksToRunByID.put(channeledTasksGroup.get(0).id, channeledTasksGroup.get(0));
			availableWorkersTmp--;
		}

		//  second - if there are still available threads, give'em to the rest of the NON_CONCURRENT_TASKS_GROUP
		List<TaskInternal> nonConcurrentTasks = tasksGroupedByConcurrencyKeys.getOrDefault(NON_CONCURRENT_TASKS_GROUP_KEY, Collections.emptyList());
		for (TaskInternal task : nonConcurrentTasks) {
			if (availableWorkersTmp <= 0) break;
			if (!tasksToRunByID.containsKey(task.id)) {
				tasksToRunByID.put(task.id, task);
				availableWorkersTmp--;
			}
		}

		return tasksToRunByID.values();
	}

	final void handleTasks(Collection<TaskInternal> tasks, ClusterTasksDataProvider dataProvider) {
		tasks.forEach(task -> {
			if (handoutTaskToWorker(dataProvider, task)) {
				concurrencyKeysFairnessMap.put(
						task.concurrencyKey != null ? task.concurrencyKey : NON_CONCURRENT_TASKS_GROUP_KEY,
						System.currentTimeMillis());
			} else {
				logger.error("failed to hand out " + task + " (task is already marked as RUNNING)");
			}
		});

		threadsUtilizationGauge
				.labels(getType())
				.set(((double) (numberOfWorkersPerNode - availableWorkers.get())) / ((double) numberOfWorkersPerNode));
	}

	final void notifyTaskWorkerFinished(ClusterTasksDataProvider dataProvider, TaskInternal task) {
		int aWorkers = availableWorkers.incrementAndGet();
		lastTaskHandledLocalTime = System.currentTimeMillis();
		logger.debug(type + " available workers " + aWorkers);

		//  submit task for removal
		clusterTasksService.getMaintainer().submitTaskToRemove(dataProvider, task);
	}

	private boolean handoutTaskToWorker(ClusterTasksDataProvider dataProvider, TaskInternal task) {
		try {
			ClusterTasksProcessorWorker worker = new ClusterTasksProcessorWorker(dataProvider, this, task);
			workersThreadPool.execute(worker);
			int aWorkers = availableWorkers.decrementAndGet();
			if (logger.isDebugEnabled()) {
				logger.debug("processor " + getType() + " took " + task);
				logger.debug(type + " available workers " + aWorkers);
			}
			return true;
		} catch (Exception e) {
			logger.error("processor " + getType() + " failed to take " + task, e);
			return false;
		}
	}

	private final class CTPWorkersThreadFactory implements ThreadFactory {

		@Override
		public Thread newThread(Runnable runnable) {
			Thread result = new Thread(runnable);
			result.setName("CTP " + runnable.getClass().getSimpleName() + " on behalf of " + type + "; TID: " + result.getId());
			result.setDaemon(true);
			return result;
		}
	}
}