/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.impl;

import com.microfocus.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

final class ClusterTasksDispatcher implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(ClusterTasksServiceImpl.class);
	private final Counter dispatchErrors;
	private final Summary dispatchDurationSummary;
	private final ClusterTasksServiceImpl.SystemWorkersConfigurer configurer;

	private volatile boolean shuttingDown = false;

	ClusterTasksDispatcher(ClusterTasksServiceImpl.SystemWorkersConfigurer configurer) {
		if (configurer == null) {
			throw new IllegalArgumentException("configurer MUST NOT be null");
		}
		this.configurer = configurer;

		dispatchErrors = Counter.build()
				.name("cts_dispatch_errors_total_" + configurer.getInstanceID().replaceAll("-", "_"))
				.help("CTS tasks' dispatch errors counter")
				.register();
		dispatchDurationSummary = Summary.build()
				.name("cts_dispatch_duration_seconds_" + configurer.getInstanceID().replaceAll("-", "_"))
				.help("CTS tasks' dispatch duration summary")
				.register();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> shuttingDown = true, "CTS Dispatcher shutdown listener"));
	}

	@Override
	public void run() {

		//  infallible tasks dispatch round
		while (!shuttingDown) {
			//  dispatch round
			Summary.Timer dispatchTimer = dispatchDurationSummary.startTimer();
			try {
				if (configurer.isCTSServiceEnabled()) {
					runDispatch();
				}
			} catch (Throwable t) {
				dispatchErrors.inc();
				logger.error("failure within dispatch iteration; total failures: " + dispatchErrors.get(), t);
			} finally {
				dispatchTimer.observeDuration();
				breathe();
			}
		}
	}

	private void runDispatch() {
		configurer.getDataProvidersMap().forEach((providerType, provider) -> {
			if (provider.isReady()) {
				Map<String, ClusterTasksProcessorBase> availableProcessorsOfDPType = new LinkedHashMap<>();
				configurer.getProcessorsMap().forEach((processorType, processor) -> {
					if (processor.getDataProviderType().equals(providerType) && processor.isReadyToHandleTaskInternal()) {
						availableProcessorsOfDPType.put(processorType, processor);
					}
				});
				if (!availableProcessorsOfDPType.isEmpty()) {
					try {
						provider.retrieveAndDispatchTasks(availableProcessorsOfDPType);
					} catch (Throwable t) {
						dispatchErrors.inc();
						logger.error("failed to dispatch tasks in " + providerType + "; total failures: " + dispatchErrors.get(), t);
					}
				} else {
					logger.debug("no available processors powered by data provider " + providerType + " found, skipping this dispatch round");
				}
			}
		});
	}

	private void breathe() {
		Integer breathingInterval = null;
		try {
			breathingInterval = configurer.getCTSServiceConfigurer().getTasksPollIntervalMillis();
		} catch (Throwable t) {
			logger.warn("failed to obtain breathing interval from service configurer, falling back to DEFAULT (" + ClusterTasksServiceConfigurerSPI.DEFAULT_POLL_INTERVAL + ")", t);
		}
		breathingInterval = breathingInterval == null ? ClusterTasksServiceConfigurerSPI.DEFAULT_POLL_INTERVAL : breathingInterval;
		breathingInterval = Math.max(breathingInterval, ClusterTasksServiceConfigurerSPI.MINIMAL_POLL_INTERVAL);
		try {
			Thread.sleep(breathingInterval);
		} catch (InterruptedException ie) {
			logger.warn("interrupted while breathing between dispatch rounds", ie);
		}
	}
}
