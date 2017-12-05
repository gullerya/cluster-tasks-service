package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.enums.CTPPersistStatus;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskStatus;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskType;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorScheduled;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksService;
import com.microfocus.octane.cluster.tasks.api.dto.TaskToEnqueue;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

/**
 * Created by gullery on 08/05/2016.
 * <p>
 * Default implementation of ClusterTasksService
 */

public class ClusterTasksServiceImpl implements ClusterTasksService {
	private final Logger logger = LoggerFactory.getLogger(ClusterTasksServiceImpl.class);

	private final CompletableFuture<Boolean> readyPromise = new CompletableFuture<>();
	private final Map<ClusterTasksDataProviderType, ClusterTasksDataProvider> dataProvidersMap = new LinkedHashMap<>();
	private final Map<String, ClusterTasksProcessorBase> processorsMap = new LinkedHashMap<>();
	private final ExecutorService dispatcherExecutor = Executors.newSingleThreadExecutor(new ClusterTasksDispatcherThreadFactory());
	private final ExecutorService gcExecutor = Executors.newSingleThreadExecutor(new ClusterTasksGCThreadFactory());
	private final ClusterTasksDispatcher dispatcher = new ClusterTasksDispatcher();
	private final ClusterTasksGC gc = new ClusterTasksGC();
	private ClusterTasksServiceConfigurerSPI serviceConfigurer;
	private ClusterTasksServiceSchemaManager schemaManager;

	private final static Counter dispatchErrors;
	private final static Summary dispatchDurationSummary;
	private final static Counter gcErrors;
	private final static Summary gcDurationSummary;

	static {
		dispatchErrors = Counter.build()
				.name("cts_dispatch_errors_total")
				.help("CTS tasks' dispatch errors counter")
				.register();
		dispatchDurationSummary = Summary.build()
				.name("cts_dispatch_duration_seconds")
				.help("CTS tasks' dispatch duration summary")
				.register();
		gcErrors = Counter.build()
				.name("cts_gc_errors_total")
				.help("CTS GC errors counter")
				.register();
		gcDurationSummary = Summary.build()
				.name("cts_gc_duration_seconds")
				.help("CTS GC duration summary")
				.register();
	}

	@Autowired
	private void registerDataProviders(List<ClusterTasksDataProvider> dataProviders) {
		dataProviders.forEach(dataProvider -> {
			if (dataProvidersMap.containsKey(dataProvider.getType())) {
				logger.error("more than one implementation pretend to provide '" + dataProvider.getType() + "' data provider");
			} else {
				dataProvidersMap.put(dataProvider.getType(), dataProvider);
			}
		});
	}

	@Autowired(required = false)
	private void registerProcessors(List<ClusterTasksProcessorBase> processors) {
		if (processors.size() > 500) {
			throw new IllegalStateException("processors number is higher than allowed (500)");
		}

		processors.forEach(processor -> {
			String type = processor.getType();
			String className = processor.getClass().getName();
			if (type == null || type.isEmpty()) {
				logger.error("processor " + className + " rejected: type MUST NOT be null nor empty");
			} else if (type.length() > 40) {
				logger.error("processor " + className + " rejected: type MUST NOT exceed 40 characters, found " + type.length() + " (" + type + ")");
			} else if (processorsMap.containsKey(type)) {
				logger.error("processor " + className + " rejected: more than one implementation pretend to process '" + type + "' tasks");
			} else {
				processorsMap.put(type, processor);
			}
		});
	}

	@Autowired
	private void registerClusterTasksServiceConfigurer(ClusterTasksServiceConfigurerSPI serviceConfigurer, ClusterTasksServiceSchemaManager schemaManager) {
		this.serviceConfigurer = serviceConfigurer;
		this.schemaManager = schemaManager;
		logger.info("------------------------------------------------");
		logger.info("------------- Cluster Tasks Service ------------");

		if (serviceConfigurer.getConfigReadyLatch() == null) {
			initService();
		} else {
			logger.info("starting listener on configuration readiness...");
			serviceConfigurer.getConfigReadyLatch().handleAsync((value, error) -> {
				logger.info("listener on configuration readiness resolved; value: " + value + ", error: " + error);
				if (value == null || !value) {
					readyPromise.complete(false);
					if (error != null) {
						throw new IllegalStateException("hosting application failed to provide configuration", error);
					} else {
						throw new IllegalStateException("hosting application failed to provide configuration");
					}
				} else {
					initService();
				}

				return null;
			});
		}
	}

	@Override
	public CompletableFuture<Boolean> getReadyPromise() {
		return readyPromise;
	}

	@Override
	public ClusterTaskPersistenceResult[] enqueueTasks(ClusterTasksDataProviderType dataProviderType, String processorType, TaskToEnqueue... tasks) {
		if (dataProviderType == null) {
			throw new IllegalArgumentException("data provider type MUST NOT be null");
		}
		if (processorType == null || processorType.isEmpty()) {
			throw new IllegalArgumentException("processor type MUST NOT be null nor empty");
		}
		if (processorType.length() > 40) {
			throw new IllegalArgumentException("processor type MAY NOT exceed 40 characters; given " + processorType.length() + " (" + processorType + ")");
		}
		if (tasks == null || tasks.length == 0) {
			throw new IllegalArgumentException("tasks array MUST NOT be null nor empty");
		}

		ClusterTasksDataProvider dataProvider = dataProvidersMap.get(dataProviderType);
		if (dataProvider != null) {
			return dataProvidersMap.get(dataProviderType).storeTasks(processorType, tasks);
		} else {
			throw new IllegalArgumentException("unknown data provider of type '" + processorType + "'");
		}
	}

	@Deprecated
	@Override
	public int countTasks(ClusterTasksDataProviderType dataProviderType, String processorType, ClusterTaskStatus... statuses) {
		Set<ClusterTaskStatus> statusSet = Arrays.stream(statuses).collect(Collectors.toSet());
		return dataProvidersMap.get(dataProviderType).countTasks(processorType, statusSet);
	}

	private void initService() {
		logger.info("starting initialization");
		boolean proceed = true;
		if (serviceConfigurer.getAdministrativeDataSource() != null) {
			logger.info("performing schema maintenance");
			proceed = schemaManager.executeSchemaMaintenance(serviceConfigurer.getDbType(), serviceConfigurer.getAdministrativeDataSource());
		} else {
			logger.info("administrative DataSource not provided, skipping schema maintenance");
		}

		if (proceed) {
			dispatcherExecutor.execute(dispatcher);
			gcExecutor.execute(gc);
			logger.info("local tasks dispatcher initialized");

			logger.info("CTS is configured & initialized");
			readyPromise.complete(true);

			ensureScheduledTasksInitialized();
			logger.info("scheduled tasks initialization verified");
		} else {
			logger.error("CTS initialization failed (failed to execute schema maintenance) and won't run");
			readyPromise.complete(false);
		}
	}

	private void ensureScheduledTasksInitialized() {
		processorsMap.forEach((type, processor) -> {
			if (processor instanceof ClusterTasksProcessorScheduled) {
				logger.info("performing initial scheduled task upsert for the first-ever-run case on behalf of " + type);
				ClusterTaskPersistenceResult enqueueResult;
				int maxEnqueueAttempts = 20, enqueueAttemptsCount = 0;
				TaskToEnqueue scheduledTask = new TaskToEnqueue();
				scheduledTask.setTaskType(ClusterTaskType.SCHEDULED);
				scheduledTask.setUniquenessKey(type);
				scheduledTask.setMaxTimeToRunMillis(((ClusterTasksProcessorScheduled) processor).getMaxTimeToRun());
				do {
					enqueueAttemptsCount++;
					enqueueResult = enqueueTasks(processor.getDataProviderType(), type, scheduledTask)[0];
					if (enqueueResult.getStatus() == CTPPersistStatus.SUCCESS) {
						logger.info("initial task for " + type + " created");
						break;
					} else if (enqueueResult.getStatus() == CTPPersistStatus.UNIQUE_CONSTRAINT_FAILURE) {
						logger.info("failed to create initial scheduled task for " + type + " with unique constraint violation, assuming that task was already created, will not reattempt");
						break;
					} else {
						logger.error("failed to create scheduled task for " + type + " with error " + enqueueResult.getStatus() + "; will reattempt for more " + (maxEnqueueAttempts - enqueueAttemptsCount) + " times");
						try {
							Thread.sleep(3000);
						} catch (InterruptedException ie) {
							logger.warn("interrupted while breathing, proceeding with reattempts");
						}
					}
				} while (enqueueAttemptsCount < maxEnqueueAttempts);
			}
		});
	}

	private final static class ClusterTasksDispatcherThreadFactory implements ThreadFactory {
		@Override
		public Thread newThread(Runnable runnable) {
			Thread result = new Thread(runnable);
			result.setName("CTP Dispatcher; TID: " + result.getId());
			result.setDaemon(true);
			return result;
		}
	}

	private final class ClusterTasksDispatcher implements Runnable {

		@Override
		public void run() {

			//  infallible tasks dispatch round
			while (true) {
				//  dispatch round
				Summary.Timer dispatchTimer = dispatchDurationSummary.startTimer();
				try {
					runDispatch();
				} catch (Throwable t) {
					dispatchErrors.inc();
					logger.error("failure within dispatch iteration; total failures: " + dispatchErrors.get(), t);
				} finally {
					dispatchTimer.observeDuration();
				}

				//  breathing pause
				Integer breathingInterval = null;
				try {
					breathingInterval = serviceConfigurer.getTasksPollIntervalMillis();
				} catch (Throwable t) {
					logger.warn("failed to obtain breathing interval from service configurer, falling back to DEFAULT (" + serviceConfigurer.DEFAULT_POLL_INTERVAL + ")", t);
				}
				breathingInterval = breathingInterval == null ? serviceConfigurer.DEFAULT_POLL_INTERVAL : breathingInterval;
				breathingInterval = Math.max(breathingInterval, serviceConfigurer.MINIMAL_POLL_INTERVAL);
				try {
					Thread.sleep(breathingInterval);
				} catch (InterruptedException ie) {
					logger.warn("interrupted while breathing between dispatch rounds", ie);
				}
			}
		}

		private void runDispatch() {
			dataProvidersMap.forEach((providerType, provider) -> {
				Map<String, ClusterTasksProcessorBase> availableProcessorsOfDPType = new LinkedHashMap<>();
				processorsMap.forEach((processorType, processor) -> {
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
			});
		}
	}

	private final static class ClusterTasksGCThreadFactory implements ThreadFactory {
		@Override
		public Thread newThread(Runnable runnable) {
			Thread result = new Thread(runnable);
			result.setName("CTP GC; TID: " + result.getId());
			result.setDaemon(true);
			return result;
		}
	}

	private final class ClusterTasksGC implements Runnable {

		@Override
		public void run() {

			//  infallible GC round
			while (true) {
				Summary.Timer gcTimer = gcDurationSummary.startTimer();
				try {
					dataProvidersMap.forEach((dpType, dataProvider) -> dataProvider.handleGarbageAndStaled());
				} catch (Throwable t) {
					gcErrors.inc();
					logger.error("failed to perform GC round; total failures: " + gcErrors.get(), t);
				} finally {
					gcTimer.observeDuration();

					Integer gcInterval = null;
					try {
						gcInterval = serviceConfigurer.getGCIntervalMillis();
					} catch (Throwable t) {
						logger.error("failed to obtain GC interval from hosting application, falling back to default (" + ClusterTasksServiceConfigurerSPI.DEFAULT_GC_INTERVAL + ")", t);
					}
					gcInterval = gcInterval == null ? ClusterTasksServiceConfigurerSPI.DEFAULT_GC_INTERVAL : gcInterval;
					gcInterval = Math.max(gcInterval, ClusterTasksServiceConfigurerSPI.MINIMAL_GC_INTERVAL);
					try {
						Thread.sleep(gcInterval);
					} catch (InterruptedException ie) {
						logger.warn("interrupted while breathing between GC rounds", ie);
					}
				}
			}
		}
	}
}
