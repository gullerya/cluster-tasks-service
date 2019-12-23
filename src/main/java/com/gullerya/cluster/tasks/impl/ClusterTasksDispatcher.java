package com.gullerya.cluster.tasks.impl;

import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

final class ClusterTasksDispatcher extends ClusterTasksInternalWorker {
	private final static Logger logger = LoggerFactory.getLogger(ClusterTasksDispatcher.class);
	private final static Integer DEFAULT_DISPATCH_INTERVAL = 1023;

	private final Counter dispatchErrors;
	private final Summary dispatchDurationSummary;

	ClusterTasksDispatcher(ClusterTasksServiceImpl.SystemWorkersConfigurer configurer) {
		super(configurer);
		String RUNTIME_INSTANCE_ID = configurer.getInstanceID();
		dispatchErrors = Counter.build()
				.name("cts_dispatch_errors_total_" + RUNTIME_INSTANCE_ID)
				.help("CTS tasks' dispatch errors counter")
				.register();
		dispatchDurationSummary = Summary.build()
				.name("cts_dispatch_duration_seconds_" + RUNTIME_INSTANCE_ID)
				.help("CTS tasks' dispatch duration summary")
				.register();
	}

	@Override
	void performWorkCycle() {
		Summary.Timer dispatchTimer = dispatchDurationSummary.startTimer();
		try {
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
		} catch (Throwable t) {
			dispatchErrors.inc();
			logger.error("failure within dispatch iteration; total failures: " + dispatchErrors.get(), t);
		} finally {
			dispatchTimer.observeDuration();
		}
	}

	Integer getEffectiveBreathingInterval() {
		return DEFAULT_DISPATCH_INTERVAL;
	}
}
