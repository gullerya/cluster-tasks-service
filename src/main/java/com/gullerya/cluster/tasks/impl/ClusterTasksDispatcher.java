package com.gullerya.cluster.tasks.impl;

import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

final class ClusterTasksDispatcher extends ClusterTasksInternalWorker {
	private final Logger logger = LoggerFactory.getLogger(ClusterTasksDispatcher.class);
	private final static Integer DEFAULT_DISPATCH_INTERVAL = 1023;
	private final static Counter dispatchErrors;
	private final static Summary dispatchDurationSummary;

	private final String RUNTIME_INSTANCE_ID;

	static {
		dispatchErrors = Counter.build()
				.name("cts_dispatch_errors_total")
				.help("CTS tasks' dispatch errors counter")
				.labelNames("runtime_instance_id")
				.register();
		dispatchDurationSummary = Summary.build()
				.name("cts_dispatch_duration_seconds")
				.help("CTS tasks' dispatch duration summary")
				.labelNames("runtime_instance_id")
				.register();
	}

	ClusterTasksDispatcher(ClusterTasksServiceImpl.SystemWorkersConfigurer configurer) {
		super(configurer);
		RUNTIME_INSTANCE_ID = configurer.getInstanceID();
	}

	@Override
	void performWorkCycle() {
		Summary.Timer dispatchTimer = dispatchDurationSummary.labels(RUNTIME_INSTANCE_ID).startTimer();
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
							dispatchErrors.labels(RUNTIME_INSTANCE_ID).inc();
							logger.error("failed to dispatch tasks in " + providerType + "; total failures: " + dispatchErrors.labels(RUNTIME_INSTANCE_ID).get(), t);
						}
					} else {
						logger.debug("no available processors powered by data provider " + providerType + " found, skipping this dispatch round");
					}
				}
			});
		} catch (Throwable t) {
			dispatchErrors.labels(RUNTIME_INSTANCE_ID).inc();
			logger.error("failure within dispatch iteration; total failures: " + dispatchErrors.labels(RUNTIME_INSTANCE_ID).get(), t);
		} finally {
			dispatchTimer.observeDuration();
		}
	}

	Integer getEffectiveBreathingInterval() {
		return DEFAULT_DISPATCH_INTERVAL;
	}
}
