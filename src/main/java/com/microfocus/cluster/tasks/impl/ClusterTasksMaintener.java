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
	private long lastTasksCountTime = 0;
	private final Counter maintenanceErrors;
	private final Summary maintenanceDurationSummary;
	private final Gauge pendingTasksCounter;

	private final ClusterTasksServiceImpl.SystemWorkersConfigurer configurer;

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
	}

	@Override
	public void run() {

		//  infallible maintenance round
		while (true) {
			Summary.Timer maintenanceTimer = maintenanceDurationSummary.startTimer();
			try {
				if (configurer.isCTSServiceEnabled()) {
					configurer.getDataProvidersMap().forEach((dpType, provider) -> {
						if (provider.isReady()) {

							//  [YG] TODO: split rescheduling from GC, this will most likely allow remove some locking
							provider.handleGarbageAndStaled();

							//  upon once-in-a-while decision - do count tasks
							if (System.currentTimeMillis() - lastTasksCountTime > ClusterTasksServiceConfigurerSPI.DEFAULT_TASKS_COUNT_INTERVAL) {
								lastTasksCountTime = System.currentTimeMillis();
								countTasks(provider);
							}
						}
					});
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

	private void countTasks(ClusterTasksDataProvider dataProvider) {
		try {
			Map<String, Integer> pendingTasksCounters = dataProvider.countTasks(ClusterTaskStatus.PENDING);
			pendingTasksCounters.forEach((processorType, count) -> pendingTasksCounter.labels(processorType).set(count));
		} catch (Exception e) {
			logger.error("failed to count tasks", e);
		}
	}

	private void breathe() {
		Integer maintenanceInterval = null;
		try {
			maintenanceInterval = configurer.getCTSServiceConfigurer().getMaintenanceIntervalMillis();
		} catch (Throwable t) {
			logger.error("failed to obtain maintenance interval from hosting application, falling back to default (" + ClusterTasksServiceConfigurerSPI.DEFAULT_MAINTENANCE_INTERVAL + ")", t);
		}
		maintenanceInterval = maintenanceInterval == null ? ClusterTasksServiceConfigurerSPI.DEFAULT_MAINTENANCE_INTERVAL : maintenanceInterval;
		maintenanceInterval = Math.max(maintenanceInterval, ClusterTasksServiceConfigurerSPI.MINIMAL_MAINTENANCE_INTERVAL);
		try {
			Thread.sleep(maintenanceInterval);
		} catch (InterruptedException ie) {
			logger.warn("interrupted while breathing between maintenance rounds", ie);
		}
	}
}
