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

import java.util.Map;

final class ClusterTasksMaintener implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(ClusterTasksServiceImpl.class);
	private final Counter maintenanceErrors;
	private final Summary maintenanceDurationSummary;
	private final Gauge pendingTasksCounter;
	private final Gauge taskBodiesCounter;
	private final ClusterTasksServiceImpl.SystemWorkersConfigurer configurer;

	private long lastTasksCountTime = 0;
	private long lastTimeRemovedNonActiveNodes = 0;
	private volatile boolean shuttingDown = false;

	ClusterTasksMaintener(ClusterTasksServiceImpl.SystemWorkersConfigurer configurer) {
		if (configurer == null) {
			throw new IllegalArgumentException("configurer MUST NOT be null");
		}
		this.configurer = configurer;

		maintenanceErrors = Counter.build()
				.name("cts_gc_errors_total_" + configurer.getInstanceID().replaceAll("-", "_"))
				.help("CTS maintenance errors counter")
				.register();
		maintenanceDurationSummary = Summary.build()
				.name("cts_gc_duration_seconds_" + configurer.getInstanceID().replaceAll("-", "_"))
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

		Runtime.getRuntime().addShutdownHook(new Thread(() -> shuttingDown = true, "CTS Maintainer shutdown listener"));
	}

	@Override
	public void run() {

		//  infallible maintenance round
		while (!shuttingDown) {
			Summary.Timer maintenanceTimer = maintenanceDurationSummary.startTimer();
			try {
				if (configurer.isCTSServiceEnabled()) {
					for (ClusterTasksDataProvider provider : configurer.getDataProvidersMap().values()) {
						if (provider.isReady()) {

							//  maintain active nodes
							maintainActiveNodes(provider);

							//  [YG] TODO: split rescheduling from GC, this will most likely allow remove some locking
							provider.handleGarbageAndStaled();

							//  update tasks related counters
							maintainTasksCounters(provider);
						}
					}
				}
			} catch (Throwable t) {
				maintenanceErrors.inc();
				logger.error("failed to perform maintenance round; total failures: " + maintenanceErrors.get(), t);
			} finally {
				maintenanceTimer.observeDuration();
				breathe();
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
			long maxTimeNoSee = getEffectiveMaintenanceInterval() * 3;
			if (System.currentTimeMillis() - lastTimeRemovedNonActiveNodes > maxTimeNoSee) {
				dataProvider.removeLongTimeNoSeeNodes(getEffectiveMaintenanceInterval() * 3);
				lastTimeRemovedNonActiveNodes = System.currentTimeMillis();
			}
		} catch (Exception e) {
			logger.error("failed to remove long time no see nodes", e);
		}
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

	private void breathe() {
		try {
			Integer maintenanceInterval = getEffectiveMaintenanceInterval();
			Thread.sleep(maintenanceInterval);
		} catch (InterruptedException ie) {
			logger.warn("interrupted while breathing between maintenance rounds", ie);
		}
	}

	private Integer getEffectiveMaintenanceInterval() {
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
}
