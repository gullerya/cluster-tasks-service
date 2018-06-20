package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorScheduled;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksService;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.microfocus.octane.cluster.tasks.api.builders.TaskBuilders;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.microfocus.octane.cluster.tasks.api.enums.CTPPersistStatus;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskStatus;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskType;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
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

	private final String RUNTIME_INSTANCE_ID = UUID.randomUUID().toString();
	private final CompletableFuture<Boolean> readyPromise = new CompletableFuture<>();
	private final Map<ClusterTasksDataProviderType, ClusterTasksDataProvider> dataProvidersMap = new LinkedHashMap<>();
	private final Map<String, ClusterTasksProcessorBase> processorsMap = new LinkedHashMap<>();
	private final ExecutorService dispatcherExecutor = Executors.newSingleThreadExecutor(new ClusterTasksDispatcherThreadFactory());
	private final ExecutorService maintainerExecutor = Executors.newSingleThreadExecutor(new ClusterTasksMaintainerThreadFactory());
	private final ClusterTasksDispatcher dispatcher = new ClusterTasksDispatcher();
	private final ClusterTasksMaintainer maintainer = new ClusterTasksMaintainer();
	private ClusterTasksServiceConfigurerSPI serviceConfigurer;
	private ClusterTasksServiceSchemaManager schemaManager;

	private static final Counter dispatchErrors;
	private static final Summary dispatchDurationSummary;
	private static final Counter maintenanceErrors;
	private static final Summary maintenanceDurationSummary;
	private static final Gauge pendingTasksCounter;

	static {
		dispatchErrors = Counter.build()
				.name("cts_dispatch_errors_total")
				.help("CTS tasks' dispatch errors counter")
				.register();
		dispatchDurationSummary = Summary.build()
				.name("cts_dispatch_duration_seconds")
				.help("CTS tasks' dispatch duration summary")
				.register();
		maintenanceErrors = Counter.build()
				.name("cts_gc_errors_total")
				.help("CTS maintenance errors counter")
				.register();
		maintenanceDurationSummary = Summary.build()
				.name("cts_gc_duration_seconds")
				.help("CTS maintenance duration summary")
				.register();
		pendingTasksCounter = Gauge.build()
				.name("cts_pending_tasks_counter")
				.help("CTS pending tasks counter (by CTP type)")
				.labelNames("processor_type")
				.register();
	}

	private static final long MAX_TIME_TO_RUN_DEFAULT = 1000 * 60;

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
		if (!processorsMap.isEmpty()) {
			logger.info(processorsMap.size() + " CTPs are registered in this instance:");
			processorsMap.keySet().forEach(key -> logger.info("\t\t" + key));
		} else {
			logger.info("none CTPs are registered in this instance");
		}
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
	public String getInstanceID() {
		return RUNTIME_INSTANCE_ID;
	}

	@Override
	public CompletableFuture<Boolean> getReadyPromise() {
		return readyPromise;
	}

	@Override
	public ClusterTaskPersistenceResult[] enqueueTasks(ClusterTasksDataProviderType dataProviderType, String processorType, ClusterTask... tasks) {
		if (!readyPromise.isDone()) {
			throw new IllegalStateException("cluster tasks service has not yet been initialized; either postpone tasks submission or listen to completion of [clusterTasksService].getReadyPromise()");
		}
		if (readyPromise.isCompletedExceptionally()) {
			throw new IllegalStateException("cluster tasks service failed to initialize; check previous logs for a root cause");
		}

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
			TaskInternal[] taskInternals = convertTasks(tasks, processorType);
			return dataProvidersMap.get(dataProviderType).storeTasks(taskInternals);
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
			setupDataProviders();

			dispatcherExecutor.execute(dispatcher);
			maintainerExecutor.execute(maintainer);
			logger.info("tasks dispatcher and maintenance threads initialized");

			logger.info("CTS is configured & initialized, instance ID: " + RUNTIME_INSTANCE_ID);
			readyPromise.complete(true);

			ensureScheduledTasksInitialized();
			logger.info("scheduled tasks initialization verified");
		} else {
			logger.error("CTS initialization failed (failed to execute schema maintenance) and won't run");
			readyPromise.complete(false);
		}
	}

	private void setupDataProviders() {
		//  DB
		if (serviceConfigurer.getDbType() != null) {
			if (serviceConfigurer.getDataSource() == null) {
				throw new IllegalStateException("DataSource is not provided, while DBType declared to be '" + serviceConfigurer.getDbType() + "'");
			}
			switch (serviceConfigurer.getDbType()) {
				case MSSQL:
					dataProvidersMap.put(ClusterTasksDataProviderType.DB, new MsSqlDbDataProvider(this, serviceConfigurer));
					break;
				case ORACLE:
					dataProvidersMap.put(ClusterTasksDataProviderType.DB, new OracleDbDataProvider(this, serviceConfigurer));
					break;
				case POSTGRESQL:
					dataProvidersMap.put(ClusterTasksDataProviderType.DB, new PostgreSqlDbDataProvider(this, serviceConfigurer));
					break;
				default:
					logger.error("DB type '" + serviceConfigurer.getDbType() + "' has no data provider, DB oriented tasking won't be available");
					break;
			}
			dataProvidersMap.get(ClusterTasksDataProviderType.DB).isReady();
		}

		//  summary
		if (!dataProvidersMap.isEmpty()) {
			logger.info("summarizing registered data providers:");
			dataProvidersMap.forEach((type, provider) -> logger.info("\t\t" + type + ": " + provider.getClass().getSimpleName()));
		} else {
			throw new IllegalStateException("no (relevant) data providers available");
		}
	}

	private void ensureScheduledTasksInitialized() {
		processorsMap.forEach((type, processor) -> {
			if (processor instanceof ClusterTasksProcessorScheduled) {
				logger.info("performing initial scheduled task upsert for the first-ever-run case on behalf of " + type);
				ClusterTasksDataProvider dataProvider = dataProvidersMap.get(processor.getDataProviderType());
				ClusterTaskPersistenceResult enqueueResult;
				int maxEnqueueAttempts = 20, enqueueAttemptsCount = 0;
				ClusterTask clusterTask = TaskBuilders.uniqueTask()
						.setUniquenessKey(type)
						.setMaxTimeToRunMillis(((ClusterTasksProcessorScheduled) processor).getMaxTimeToRun())
						.build();
				TaskInternal[] scheduledTasks = convertTasks(new ClusterTask[]{clusterTask}, type);
				scheduledTasks[0].taskType = ClusterTaskType.SCHEDULED;
				do {
					enqueueAttemptsCount++;
					enqueueResult = dataProvider.storeTasks(scheduledTasks[0])[0];
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

	private TaskInternal[] convertTasks(ClusterTask[] sourceTasks, String targetProcessorType) {
		TaskInternal[] result = new TaskInternal[sourceTasks.length];
		for (int i = 0; i < sourceTasks.length; i++) {
			ClusterTask source = sourceTasks[i];
			if (source == null) {
				throw new IllegalArgumentException("of the submitted tasks NONE SHOULD BE NULL");
			}

			TaskInternal target = new TaskInternal();

			if (source.getUniquenessKey() != null) {
				target.uniquenessKey = source.getUniquenessKey();
				target.concurrencyKey = source.getUniquenessKey();
				if (source.getConcurrencyKey() != null) {
					logger.warn("concurrency key MUST NOT be used along with uniqueness key, falling back to uniqueness key as concurrency key");
				}
			} else {
				target.uniquenessKey = UUID.randomUUID().toString();
				target.concurrencyKey = source.getConcurrencyKey();
			}

			target.processorType = targetProcessorType;
			target.orderingFactor = null;
			target.delayByMillis = source.getDelayByMillis() == null ? 0L : source.getDelayByMillis();
			target.maxTimeToRunMillis = source.getMaxTimeToRunMillis() == null || source.getMaxTimeToRunMillis() == 0 ? MAX_TIME_TO_RUN_DEFAULT : source.getMaxTimeToRunMillis();
			target.body = source.getBody() == null || source.getBody().isEmpty() ? null : source.getBody();
			target.taskType = ClusterTaskType.REGULAR;

			result[i] = target;
		}

		return result;
	}

	//  INTERNAL WORKERS: DISPATCHER AND MAINTAINER
	//
	private final class ClusterTasksDispatcherThreadFactory implements ThreadFactory {
		@Override
		public Thread newThread(Runnable runnable) {
			Thread result = new Thread(runnable);
			result.setName("CTS Dispatcher; TID: " + result.getId());
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
					//  [YG] TODO: add here a monitor for how much time call to foreign isEnabled lasts (to notify on very prolonged calls)
					if (serviceConfigurer.isEnabled()) {
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
			dataProvidersMap.forEach((providerType, provider) -> {
				if (provider.isReady()) {
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
				}
			});
		}

		private void breathe() {
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

	private final class ClusterTasksMaintainerThreadFactory implements ThreadFactory {
		@Override
		public Thread newThread(Runnable runnable) {
			Thread result = new Thread(runnable);
			result.setName("CTS Maintainer; TID: " + result.getId());
			result.setDaemon(true);
			return result;
		}
	}

	private final class ClusterTasksMaintainer implements Runnable {
		private long lastTasksCountTime = 0;

		@Override
		public void run() {

			//  infallible maintenance round
			while (true) {
				Summary.Timer maintenanceTimer = maintenanceDurationSummary.startTimer();
				try {
					//  [YG] TODO: add here a monitor for how much time call to foreign isEnabled lasts (to notify on very prolonged calls)
					if (serviceConfigurer.isEnabled()) {
						dataProvidersMap.forEach((dpType, dataProvider) -> {
							if (dataProvider.isReady()) {

								//  [YG] TODO: split rescheduling from GC, this will most likely allow remove some locking
								dataProvider.handleGarbageAndStaled();

								//  upon once-in-a-while decision - do count tasks
								if (System.currentTimeMillis() - lastTasksCountTime > ClusterTasksServiceConfigurerSPI.DEFAULT_TASKS_COUNT_INTERVAL) {
									lastTasksCountTime = System.currentTimeMillis();
									countTasks(dataProvider);
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
				maintenanceInterval = serviceConfigurer.getMaintenanceIntervalMillis();
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
}
