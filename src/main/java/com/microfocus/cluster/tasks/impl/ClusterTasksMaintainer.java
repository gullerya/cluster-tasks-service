/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.impl;

import com.microfocus.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskStatus;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class ClusterTasksMaintainer extends ClusterTasksInternalWorker {
	private final Logger logger = LoggerFactory.getLogger(ClusterTasksMaintainer.class);
	private final Counter maintenanceErrors;
	private final Summary maintenanceDurationSummary;
	private final Gauge pendingTasksCounter;
	private final Gauge taskBodiesCounter;

	private final Map<ClusterTasksDataProvider, Map<Long, List<Long>>> taskBodiesToRemove = new HashMap<>();

	private long lastTasksCountTime = 0;
	private long lastTimeRemovedNonActiveNodes = 0;

	ClusterTasksMaintainer(ClusterTasksServiceImpl.SystemWorkersConfigurer configurer) {
		super(configurer);

		maintenanceErrors = Counter.build()
				.name("cts_maintenance_errors_total_" + configurer.getInstanceID().replaceAll("-", "_"))
				.help("CTS maintenance errors counter")
				.register();
		maintenanceDurationSummary = Summary.build()
				.name("cts_maintenance_duration_seconds_" + configurer.getInstanceID().replaceAll("-", "_"))
				.help("CTS maintenance duration summary")
				.register();
		pendingTasksCounter = Gauge.build()
				.name("cts_pending_tasks_counter_" + configurer.getInstanceID().replaceAll("-", "_"))
				.help("CTS pending tasks counter (by CTP type)")
				.labelNames("processor_type")
				.register();
		taskBodiesCounter = Gauge.build()
				.name("cts_task_bodies_counter_" + configurer.getInstanceID().replaceAll("-", "_"))
				.help("CTS task bodies counter (per partition)")
				.labelNames("partition")
				.register();
	}

	@Override
	void performWorkCycle() {
		Summary.Timer maintenanceTimer = maintenanceDurationSummary.startTimer();
		try {
			for (ClusterTasksDataProvider provider : configurer.getDataProvidersMap().values()) {
				if (provider.isReady()) {
					maintainActiveNodes(provider);
					maintainFinishedAndStale(provider);
					maintainTasksCounters(provider);
				}
			}
		} catch (Throwable t) {
			maintenanceErrors.inc();
			logger.error("failed to perform maintenance round; total failures: " + maintenanceErrors.get(), t);
		} finally {
			maintenanceTimer.observeDuration();
		}
	}

	@Override
	Integer getEffectiveBreathingInterval() {
		Integer result = null;
		try {
			result = configurer.getCTSServiceConfigurer().getMaintenanceIntervalMillis();
		} catch (Throwable t) {
			logger.error("failed to obtain maintenance interval from hosting application, falling back to default (" + ClusterTasksServiceConfigurerSPI.DEFAULT_MAINTENANCE_INTERVAL + ")", t);
		}
		result = result == null
				? ClusterTasksServiceConfigurerSPI.DEFAULT_MAINTENANCE_INTERVAL
				: Math.max(result, ClusterTasksServiceConfigurerSPI.MINIMAL_MAINTENANCE_INTERVAL);
		return result;
	}

	void submitTaskToRemove(ClusterTasksDataProvider dataProvider, TaskInternal task) {
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

		//  remove inactive nodes (node will be considered inactive if it has not been see for X3 times maintenance interval)
		try {
			if (System.currentTimeMillis() - lastTimeRemovedNonActiveNodes > getEffectiveBreathingInterval() * 3) {
				int affected = dataProvider.removeLongTimeNoSeeNodes(getEffectiveBreathingInterval() * 4);
				if (affected > 0) {
					logger.info("found and removed " + affected + " non-active nodes");
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
		if (System.currentTimeMillis() - lastTasksCountTime > ClusterTasksServiceConfigurerSPI.DEFAULT_TASKS_COUNT_INTERVAL) {

			//  count pending tasks
			try {
				Map<String, Integer> pendingTasksCounters = dataProvider.countTasks(ClusterTaskStatus.PENDING);
				for (Map.Entry<String, Integer> counter : pendingTasksCounters.entrySet()) {
					pendingTasksCounter.labels(counter.getKey()).set(counter.getValue());
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
