/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.impl;

import com.microfocus.cluster.tasks.api.enums.ClusterTaskStatus;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class ClusterTasksMaintainer extends ClusterTasksInternalWorker {
	private final Logger logger = LoggerFactory.getLogger(ClusterTasksMaintainer.class);
	private final static Integer DEFAULT_MAINTENANCE_INTERVAL = 17039;
	private final static Integer DEFAULT_TASKS_COUNT_INTERVAL = 32204;
	private final static Counter maintenanceErrors;
	private final static Summary maintenanceDurationSummary;
	private final static Gauge pendingTasksCounter;
	private final static Gauge taskBodiesCounter;

	private final String RUNTIME_INSTANCE_ID;

	private final Set<String> everKnownTaskProcessors = new HashSet<>();
	private final Map<ClusterTasksDataProvider, Map<Long, List<Long>>> taskBodiesToRemove = new HashMap<>();

	private long lastTasksCountTime = 0;
	private long lastTimeRemovedNonActiveNodes = 0;
	private int customMaintenanceInterval = 0;

	static {
		maintenanceErrors = Counter.build()
				.name("cts_maintenance_errors_total")
				.help("CTS maintenance errors counter")
				.labelNames("runtime_instance_id")
				.register();
		maintenanceDurationSummary = Summary.build()
				.name("cts_maintenance_duration_seconds")
				.help("CTS maintenance duration summary")
				.labelNames("runtime_instance_id")
				.register();
		pendingTasksCounter = Gauge.build()
				.name("cts_pending_tasks_counter")
				.help("CTS pending tasks counter (by CTP type)")
				.labelNames("processor_type")
				.register();
		taskBodiesCounter = Gauge.build()
				.name("cts_task_bodies_counter")
				.help("CTS task bodies counter (per partition)")
				.labelNames("partition")
				.register();
	}

	ClusterTasksMaintainer(ClusterTasksServiceImpl.SystemWorkersConfigurer configurer) {
		super(configurer);
		RUNTIME_INSTANCE_ID = configurer.getInstanceID();
	}

	@Override
	void performWorkCycle() {
		Summary.Timer maintenanceTimer = maintenanceDurationSummary.labels(RUNTIME_INSTANCE_ID).startTimer();
		try {
			for (ClusterTasksDataProvider provider : configurer.getDataProvidersMap().values()) {
				if (provider.isReady()) {
					maintainActiveNodes(provider);
					maintainFinishedAndStale(provider);
					maintainTasksCounters(provider);
				}
			}
		} catch (Throwable t) {
			maintenanceErrors.labels(RUNTIME_INSTANCE_ID).inc();
			logger.error("failed to perform maintenance round; total failures: " + maintenanceErrors.labels(RUNTIME_INSTANCE_ID).get(), t);
		} finally {
			maintenanceTimer.observeDuration();
		}
	}

	@Override
	Integer getEffectiveBreathingInterval() {
		return customMaintenanceInterval == 0 ? DEFAULT_MAINTENANCE_INTERVAL : customMaintenanceInterval;
	}

	void setMaintenanceInterval(int maintenanceInterval) {
		customMaintenanceInterval = maintenanceInterval;
	}

	void submitTaskToRemove(ClusterTasksDataProvider dataProvider, ClusterTaskImpl task) {
		//  store data to remove task's body
		if (task.partitionIndex != null) {
			synchronized (taskBodiesToRemove) {
				taskBodiesToRemove
						.computeIfAbsent(dataProvider, dp -> new HashMap<>())
						.computeIfAbsent(task.partitionIndex, pi -> new ArrayList<>())
						.add(task.id);
			}
		}
	}

	private void maintainActiveNodes(ClusterTasksDataProvider dataProvider) {
		//  update self as active
		try {
			dataProvider.updateSelfLastSeen(configurer.getInstanceID());
		} catch (Exception e) {
			logger.error("failed to update this node's (" + configurer.getInstanceID() + ") last seen", e);
		}

		//  remove inactive nodes
		//  - the verification will run once in X3 cycle time
		//  - the node will be considered inactive if last seen before X4 cycle time
		try {
			if (System.currentTimeMillis() - lastTimeRemovedNonActiveNodes > getEffectiveBreathingInterval() * 3) {
				int affected = dataProvider.removeLongTimeNoSeeNodes(getEffectiveBreathingInterval() * 4);
				if (affected > 0) {
					logger.info("found and removed " + affected + " non-active node/s");
				}
				lastTimeRemovedNonActiveNodes = System.currentTimeMillis();
			}
		} catch (Exception e) {
			logger.error("failed to remove long time no see nodes", e);
		}
	}

	private void maintainFinishedAndStale(ClusterTasksDataProvider dataProvider) {
		//  clean up bodies KNOWN to be pending for removal (accumulated in memory)
		if (taskBodiesToRemove.containsKey(dataProvider)) {
			taskBodiesToRemove.get(dataProvider).forEach((partitionIndex, taskBodies) -> {
				if (!taskBodies.isEmpty()) {
					Long[] tmpTaskBodies;
					synchronized (taskBodiesToRemove) {
						tmpTaskBodies = taskBodies.toArray(new Long[0]);
						taskBodies.clear();
					}
					dataProvider.cleanFinishedTaskBodiesByIDs(partitionIndex, tmpTaskBodies);
				}
			});
		}

		//  collect and process staled tasks
		dataProvider.handleStaledTasks();
	}

	private void maintainTasksCounters(ClusterTasksDataProvider dataProvider) {
		if (System.currentTimeMillis() - lastTasksCountTime > DEFAULT_TASKS_COUNT_INTERVAL) {

			//  count pending tasks
			try {
				Map<String, Integer> pendingTasksCounters = dataProvider.countTasks(ClusterTaskStatus.PENDING);
				for (Map.Entry<String, Integer> counter : pendingTasksCounters.entrySet()) {
					pendingTasksCounter.labels(counter.getKey()).set(counter.getValue());
				}
				everKnownTaskProcessors.addAll(pendingTasksCounters.keySet());          //  adding all task processors to the cached set
				for (String knownTaskProcessor : everKnownTaskProcessors) {
					if (!pendingTasksCounters.containsKey(knownTaskProcessor)) {
						pendingTasksCounter.labels(knownTaskProcessor).set(0);          //  zeroing value for known task processor that got no data this round
					}
				}
			} catch (Exception e) {
				logger.error("failed to count tasks", e);
			}

			//  count task bodies
			try {
				Map<String, Integer> taskBodiesCounters = dataProvider.countBodies();
				for (Map.Entry<String, Integer> counter : taskBodiesCounters.entrySet()) {
					taskBodiesCounter.labels(counter.getKey()).set(counter.getValue());
				}
			} catch (Exception e) {
				logger.error("failed to count task bodies", e);
			}

			lastTasksCountTime = System.currentTimeMillis();
		}
	}
}
