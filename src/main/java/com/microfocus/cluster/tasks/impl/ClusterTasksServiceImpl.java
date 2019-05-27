/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.impl;

import com.microfocus.cluster.tasks.api.ClusterTasksProcessorScheduled;
import com.microfocus.cluster.tasks.api.ClusterTasksService;
import com.microfocus.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.microfocus.cluster.tasks.api.builders.TaskBuilders;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskInsertStatus;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskStatus;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskType;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

/**
 * Created by gullery on 08/05/2016.
 * <p>
 * Default implementation of ClusterTasksService
 */

public class ClusterTasksServiceImpl implements ClusterTasksService {
	private final Logger logger = LoggerFactory.getLogger(ClusterTasksServiceImpl.class);
	private final static Gauge tasksInsertionAverageDuration;
	private static final Histogram foreignIsEnabledCallDuration;

	private final String RUNTIME_INSTANCE_ID = UUID.randomUUID().toString();
	private final CompletableFuture<Boolean> readyPromise = new CompletableFuture<>();
	private final Map<ClusterTasksDataProviderType, ClusterTasksDataProvider> dataProvidersMap = new LinkedHashMap<>();
	private final Map<String, ClusterTasksProcessorBase> processorsMap = new LinkedHashMap<>();
	private final ExecutorService dispatcherExecutor = Executors.newSingleThreadExecutor(new ClusterTasksDispatcherThreadFactory());
	private final ExecutorService maintainerExecutor = Executors.newSingleThreadExecutor(new ClusterTasksMaintainerThreadFactory());
	private final SystemWorkersConfigurer workersConfigurer = new SystemWorkersConfigurer();
	private final ClusterTasksDispatcher dispatcher = new ClusterTasksDispatcher(workersConfigurer);
	private final ClusterTasksMaintainer maintainer = new ClusterTasksMaintainer(workersConfigurer);

	private ClusterTasksServiceConfigurerSPI serviceConfigurer;
	private ClusterTasksServiceSchemaManager schemaManager;

	static {
		tasksInsertionAverageDuration = Gauge.build()
				.name("cts_task_insert_average_time")
				.help("CTS task insert average time (in millis)")
				.labelNames("runtime_instance_id")
				.register();
		foreignIsEnabledCallDuration = Histogram.build()
				.name("cts_foreign_is_enabled_duration")
				.help("CTS foreign 'isEnabled' call duration")
				.labelNames("runtime_instance_id")
				.register();
	}

	@Autowired
	private ClusterTasksServiceImpl(ClusterTasksServiceConfigurerSPI serviceConfigurer, ClusterTasksServiceSchemaManager schemaManager) {
		this.serviceConfigurer = serviceConfigurer;
		this.schemaManager = schemaManager;
		logger.info("------------------------------------------------");
		logger.info("------------- Cluster Tasks Service ------------");


		if (serviceConfigurer.getConfigReadyLatch() == null) {
			initService();
		} else {
			logger.info("starting listener on configuration readiness...");
			serviceConfigurer.getConfigReadyLatch().handleAsync((value, error) -> {
				logger.info("listener on configuration readiness resolved; value: " + value + (error != null ? (", error: " + error) : ""));
				if (value == null || !value) {
					readyPromise.complete(false);
					if (error != null) {
						logger.error("hosting application FAILED to provide configuration, this instance of CTS is not workable", error);
					} else {
						logger.error("hosting application FAILED to provide configuration, this instance of CTS is not workable");
					}
				} else {
					try {
						initService();
					} catch (Throwable throwable) {
						logger.error("FAILED to initialize ClusterTasksService, this instance of CTS is not workable", throwable);
					}
				}

				return null;
			});
		}
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
		if (!processorsMap.isEmpty()) {
			logger.info(processorsMap.size() + " CTPs are registered in this instance:");
			processorsMap.keySet().forEach(key -> logger.info("\t\t" + key));
		} else {
			logger.info("none CTPs are registered in this instance");
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
		if (!dataProvidersMap.containsKey(dataProviderType)) {
			throw new IllegalStateException("unknown data provider of type " + dataProviderType);
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
			long startStore = System.currentTimeMillis();
			ClusterTaskImpl[] taskInternals = convertTasks(tasks, processorType);
			ClusterTaskPersistenceResult[] result = dataProvidersMap.get(dataProviderType).storeTasks(taskInternals);
			long timeForAll = System.currentTimeMillis() - startStore;
			tasksInsertionAverageDuration.labels(RUNTIME_INSTANCE_ID).set((double) timeForAll / tasks.length);
			return result;
		} else {
			throw new IllegalArgumentException("unknown data provider of type '" + processorType + "'");
		}
	}

	@Override
	public void updateScheduledTaskInterval(ClusterTasksDataProviderType dataProviderType, String processorType, long newTaskRunIntervalMillis) {
		if (!readyPromise.isDone()) {
			throw new IllegalStateException("cluster tasks service has not yet been initialized; either postpone tasks submission or listen to completion of [clusterTasksService].getReadyPromise()");
		}
		if (readyPromise.isCompletedExceptionally()) {
			throw new IllegalStateException("cluster tasks service failed to initialize; check previous logs for a root cause");
		}

		if (dataProviderType == null) {
			throw new IllegalArgumentException("data provider type MUST NOT be null");
		}
		if (!dataProvidersMap.containsKey(dataProviderType)) {
			throw new IllegalStateException("unknown data provider of type " + dataProviderType);
		}
		if (processorType == null || processorType.isEmpty()) {
			throw new IllegalArgumentException("processor type MUST NOT be null nor empty");
		}

		ClusterTasksDataProvider dataProvider = dataProvidersMap.get(dataProviderType);
		dataProvider.updateScheduledTaskInterval(processorType, Math.max(0, newTaskRunIntervalMillis));
	}

	@Override
	public Future<Boolean> stop() {
		return CompletableFuture.allOf(
				dispatcher.halt(),
				maintainer.halt()
		).handleAsync((e, r) -> true);
	}

	@Override
	public int countTasksByApplicationKey(ClusterTasksDataProviderType dataProviderType, String applicationKey, ClusterTaskStatus status) {
		if (dataProviderType == null) {
			throw new IllegalArgumentException("data provider type MUST NOT be NULL");
		}
		if (!dataProvidersMap.containsKey(dataProviderType)) {
			throw new IllegalStateException("no data providers of type " + dataProviderType + " registered");
		}
		if (applicationKey != null && applicationKey.length() > TaskBuilderBase.MAX_APPLICATION_KEY_LENGTH) {
			throw new IllegalArgumentException("application key MAY NOT exceed " + TaskBuilderBase.MAX_APPLICATION_KEY_LENGTH + " chars length");
		}

		ClusterTasksDataProvider dataProvider = dataProvidersMap.get(dataProviderType);
		return dataProvider.countTasksByApplicationKey(applicationKey, status);
	}

	@Deprecated
	@Override
	public int countTasks(ClusterTasksDataProviderType dataProviderType, String processorType, ClusterTaskStatus... statuses) {
		Set<ClusterTaskStatus> statusSet = Arrays.stream(statuses).collect(Collectors.toSet());
		return dataProvidersMap.get(dataProviderType).countTasks(processorType, statusSet);
	}

	ClusterTasksMaintainer getMaintainer() {
		return maintainer;
	}

	private void initService() {
		logger.info("starting initialization");
		if (serviceConfigurer.getAdministrativeDataSource() != null) {
			logger.info("performing schema maintenance");
			schemaManager.executeSchemaMaintenance(serviceConfigurer.getDbType(), serviceConfigurer.getAdministrativeDataSource());
		} else {
			logger.info("administrative DataSource not provided, skipping schema maintenance");
		}

		setupDataProviders();

		logger.info("initialising scheduled tasks...");
		ensureScheduledTasksInitialized();
		logger.info("... scheduled tasks initialization verified");

		logger.info("initialising Dispatcher and Maintainer threads...");
		dispatcherExecutor.execute(dispatcher);
		maintainerExecutor.execute(maintainer);
		logger.info("... Dispatcher and Maintainer threads initialized");

		logger.info("CTS is configured & initialized, instance ID: " + RUNTIME_INSTANCE_ID);
		readyPromise.complete(true);
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
		processorsMap.entrySet().stream()
				.filter(entry -> entry.getValue() instanceof ClusterTasksProcessorScheduled)
				.forEach(entry -> {
					try {
						String type = entry.getKey();
						ClusterTasksProcessorBase processor = entry.getValue();
						logger.info("performing initial scheduled task upsert for the first-ever-run case on behalf of " + type);
						ClusterTasksDataProvider dataProvider = dataProvidersMap.get(processor.getDataProviderType());
						ClusterTaskPersistenceResult enqueueResult;
						int maxEnqueueAttempts = 20, enqueueAttemptsCount = 0;
						ClusterTask clusterTask = TaskBuilders.uniqueTask()
								.setUniquenessKey(type.length() > 34
										? type.substring(0, 34)
										: type)
								.setDelayByMillis(processor.scheduledTaskRunInterval)
								.build();
						ClusterTaskImpl[] scheduledTasks = convertTasks(new ClusterTask[]{clusterTask}, type);
						scheduledTasks[0].taskType = ClusterTaskType.SCHEDULED;
						do {
							enqueueAttemptsCount++;
							enqueueResult = dataProvider.storeTasks(scheduledTasks[0])[0];
							if (enqueueResult.getStatus() == ClusterTaskInsertStatus.SUCCESS) {
								logger.info("initial task for " + type + " created");
								break;
							} else if (enqueueResult.getStatus() == ClusterTaskInsertStatus.UNIQUE_CONSTRAINT_FAILURE) {
								logger.info("failed to create initial scheduled task for " + type + " with unique constraint violation, assuming that task is already present");
								if (processor.forceUpdateSchedulingInterval) {
									logger.info("task processor " + type + " said to force update run interval (specified interval is " + processor.scheduledTaskRunInterval + "), updating...");
									dataProvider.updateScheduledTaskInterval(processor.getType(), processor.scheduledTaskRunInterval);
									logger.info("... update task processor " + type + " to new interval finished");
								}
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
					} catch (RuntimeException re) {
						logger.error("failed to initialize scheduled task for processor type " + entry.getKey(), re);
					}
				});
	}

	private ClusterTaskImpl[] convertTasks(ClusterTask[] sourceTasks, String targetProcessorType) {
		ClusterTaskImpl[] result = new ClusterTaskImpl[sourceTasks.length];
		for (int i = 0; i < sourceTasks.length; i++) {
			ClusterTask source = sourceTasks[i];
			if (source == null) {
				throw new IllegalArgumentException("of the submitted tasks NONE SHOULD BE NULL");
			}

			ClusterTaskImpl target = new ClusterTaskImpl();

			//  uniqueness key
			target.uniquenessKey = source.getUniquenessKey() != null
					? source.getUniquenessKey()
					: UUID.randomUUID().toString();

			//  concurrency key
			preprocessConcurrencyKey(source, target, targetProcessorType);

			target.processorType = targetProcessorType;
			target.applicationKey = source.getApplicationKey();
			target.orderingFactor = null;
			target.delayByMillis = source.getDelayByMillis() == null ? (Long) 0L : source.getDelayByMillis();
			target.body = source.getBody() == null || source.getBody().isEmpty() ? null : source.getBody();
			target.taskType = ClusterTaskType.REGULAR;

			result[i] = target;
		}

		return result;
	}

	private void preprocessConcurrencyKey(ClusterTask source, ClusterTaskImpl target, String processorType) {
		String cKey;
		if (source.getUniquenessKey() != null) {
			cKey = source.getUniquenessKey();
			if (source.getConcurrencyKey() != null) {
				logger.warn("concurrency key MUST NOT be used along with uniqueness key, falling back to uniqueness key as concurrency key");
			}
		} else {
			cKey = source.getConcurrencyKey();
		}

		if (cKey != null && !cKey.isEmpty()) {
			if (source instanceof ClusterTaskImpl && !((ClusterTaskImpl) source).concurrencyKeyUntouched) {
				String weakHash = CTSUtils.get6CharsChecksum(processorType);
				target.concurrencyKey = cKey + weakHash;
			} else {
				target.concurrencyKey = cKey;
			}
		}
	}

	private static final class ClusterTasksDispatcherThreadFactory implements ThreadFactory {
		@Override
		public Thread newThread(Runnable runnable) {
			Thread result = new Thread(runnable);
			result.setName("CTS Dispatcher; TID: " + result.getId());
			result.setDaemon(true);
			return result;
		}
	}

	private static final class ClusterTasksMaintainerThreadFactory implements ThreadFactory {
		@Override
		public Thread newThread(Runnable runnable) {
			Thread result = new Thread(runnable);
			result.setName("CTS Maintainer; TID: " + result.getId());
			result.setDaemon(true);
			return result;
		}
	}

	/**
	 * Configurer class with a very limited creation access level, but wider read access level for protected internal configuration flows
	 * - for a most reasons this class is just a proxy for getting ClusterTasksService private properties in a safe way
	 */
	final class SystemWorkersConfigurer {
		private SystemWorkersConfigurer() {
		}

		String getInstanceID() {
			return RUNTIME_INSTANCE_ID;
		}

		boolean isServiceEnabled() {
			Histogram.Timer foreignCallTimer = foreignIsEnabledCallDuration.labels(RUNTIME_INSTANCE_ID).startTimer();
			boolean isEnabled = true;
			try {
				isEnabled = serviceConfigurer.isEnabled();
			} catch (Throwable t) {
				logger.error("failed to check 'isEnabled' by consuming application ('foreign' call)", t);
			}
			foreignCallTimer.close();
			return isEnabled;
		}

		Map<ClusterTasksDataProviderType, ClusterTasksDataProvider> getDataProvidersMap() {
			return dataProvidersMap;
		}

		Map<String, ClusterTasksProcessorBase> getProcessorsMap() {
			return processorsMap;
		}
	}
}
