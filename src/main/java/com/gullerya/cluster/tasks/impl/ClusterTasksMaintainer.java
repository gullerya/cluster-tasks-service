package com.gullerya.cluster.tasks.impl;

import com.gullerya.cluster.tasks.api.enums.ClusterTaskStatus;
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

	private final Set<String> everKnownTaskProcessors = new HashSet<>();
	private final Map<ClusterTasksDataProvider, Map<Long, List<Long>>> taskBodiesToRemove = new HashMap<>();

	private final Counter maintenanceErrors;
	private final Summary maintenanceDurationSummary;
	private final Gauge pendingTasksCounter;
	private final Gauge taskBodiesCounter;

	private long lastTasksCountTime = 0;
	private long lastTimeRemovedNonActiveNodes = 0;
	private int customMaintenanceInterval = 0;

	ClusterTasksMaintainer(ClusterTasksServiceImpl.SystemWorkersConfigurer configurer) {
		super(configurer);
		String RUNTIME_INSTANCE_ID = configurer.getInstanceID();
		maintenanceErrors = Counter.build()
				.name("cts_maintenance_errors_total_" + RUNTIME_INSTANCE_ID)
				.help("CTS maintenance errors counter")
				.register();
		maintenanceDurationSummary = Summary.build()
				.name("cts_maintenance_duration_seconds_" + RUNTIME_INSTANCE_ID)
				.help("CTS maintenance duration summary")
				.register();
		pendingTasksCounter = Gauge.build()
				.name("cts_pending_tasks_counter_" + RUNTIME_INSTANCE_ID)
				.help("CTS pending tasks counter (by CTP type)")
				.labelNames("processor_type")
				.register();
		taskBodiesCounter = Gauge.build()
				.name("cts_task_bodies_counter_" + RUNTIME_INSTANCE_ID)
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
			//  maintain storage - handles all providers itself
			maintainStorage(false);
		} catch (Throwable t) {
			maintenanceErrors.inc();
			logger.error("failed to perform maintenance round; total failures: " + maintenanceErrors.get(), t);
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

	void maintainStorage(boolean force) {
		for (ClusterTasksDataProvider provider : configurer.getDataProvidersMap().values()) {
			if (provider.isReady()) {
				provider.maintainStorage(force);
			}
		}
	}
}
