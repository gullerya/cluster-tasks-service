package com.gullerya.cluster.tasks.impl;

import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gullery on 08/05/2016.
 * <p>
 * Base class for every ClusterTasksProcessor to derive from, used for an internal inter-op between the CTService and all of the CTProcessors
 */

public abstract class ClusterTasksProcessorBase {
	private final Logger logger = LoggerFactory.getLogger(ClusterTasksProcessorBase.class);
	private static final String NON_CONCURRENT_TASKS_GROUP_KEY = "NULL";
	private static final Gauge threadsUtilizationGauge;
	private static final Histogram foreignIsReadyToHandleTasksCallDuration;
	private static final Histogram foreignIsTaskAbleToRunCallDuration;

	private final String type;
	private final ClusterTasksDataProviderType dataProviderType;
	private final AtomicInteger availableWorkers = new AtomicInteger(0);
	private final Map<String, Long> concurrencyKeysFairnessMap = new LinkedHashMap<>();
	private int numberOfWorkersPerNode;
	private int minimalTasksTakeInterval;
	private long lastTaskHandledLocalTime;
	protected long scheduledTaskRunInterval;
	protected boolean forceUpdateSchedulingInterval;

	private ExecutorService workersThreadPool;

	@Autowired
	private ClusterTasksServiceImpl clusterTasksService;

	static {
		threadsUtilizationGauge = Gauge.build()
				.name("cts_per_processor_threads_utilization_percents")
				.help("CTS per-processor threads utilization")
				.labelNames("processor_type")
				.register();
		foreignIsReadyToHandleTasksCallDuration = Histogram.build()
				.name("cts_foreign_is_ready_to_handle_tasks_duration")
				.help("CTS foreign 'isReadyToHandleTasks' call duration")
				.labelNames("runtime_instance_id")
				.register();
		foreignIsTaskAbleToRunCallDuration = Histogram.build()
				.name("cts_foreign_is_task_able_to_run_duration")
				.help("CTS foreign 'isTaskAbleToRun' call duration")
				.labelNames("runtime_instance_id")
				.register();
	}

	protected ClusterTasksProcessorBase(ClusterTasksDataProviderType dataProviderType, int numberOfWorkersPerNode) {
		this(dataProviderType, numberOfWorkersPerNode, 0);
	}

	protected ClusterTasksProcessorBase(ClusterTasksDataProviderType dataProviderType, int numberOfWorkersPerNode, int minimalTasksTakeInterval) {
		this.type = this.getClass().getSimpleName();
		this.dataProviderType = dataProviderType;
		this.numberOfWorkersPerNode = numberOfWorkersPerNode;
		this.minimalTasksTakeInterval = minimalTasksTakeInterval;
	}

	@PostConstruct
	private void initialize() {
		workersThreadPool = Executors.newFixedThreadPool(numberOfWorkersPerNode, new CTPWorkersThreadFactory());
		availableWorkers.set(numberOfWorkersPerNode);

		logger.info(this.type + " initialized: data provider type: " + dataProviderType + "; worker threads per node: " + numberOfWorkersPerNode);
	}

	//
	//  EXPOSED API / EXTENSIBILITY POINTS
	//

	/**
	 * processor's custom task processing logic
	 *
	 * @param task task that is to be processed
	 * @throws Exception processor MAY throw Exception and the service will manage it (catch, log, metrics)
	 */
	abstract public void processTask(ClusterTask task) throws Exception;

	/**
	 * updates scheduled task with new run interval
	 * - this method will also reset task CREATED time so that the interval will take effect as from NOW
	 *
	 * @param newTaskRunIntervalMillis new interval in millis
	 */
	public final void reschedule(long newTaskRunIntervalMillis) {
		clusterTasksService.updateScheduledTaskInterval(getDataProviderType(), getType(), Math.max(0, newTaskRunIntervalMillis));
	}

	/**
	 * returns processor's type key
	 * - MUST be a NON-NULL and NON-EMPTY string
	 * - any 2 different processors is the single process MUST NOT have the same type (it is possible to run different processors for the same type in different processes, FWIW)
	 * - as a default, processor's simple class name is taken, but MAY be overrode by custom logic
	 *
	 * @return processor's type (key)
	 */
	protected String getType() {
		return type;
	}

	/**
	 * sets a minimal interval that the processor is ready to take new tasks from the service
	 * - this is the lower level of processors throughput / resources utilization (along with configuration of threads per node)
	 * - delayed tasks MAY are expected to run withing the following span of time: delay - delay + tasksTakeInterval
	 *
	 * @param minimalTasksTakeInterval interval of the processor's readiness to take tasks, in milliseconds
	 */
	protected void setMinimalTasksTakeInterval(int minimalTasksTakeInterval) {
		this.minimalTasksTakeInterval = Math.max(minimalTasksTakeInterval, 0);
	}

	/**
	 * gets a processor's status as of ability to handle tasks in general (not specific one)
	 * - call to this API performed each dispatch cycle BEFORE even going to DB
	 * - delayed tasks MAY are expected to run withing the following span of time: delay - delay + tasksTakeInterval
	 *
	 * @return current condition of the processor as of readiness to take [any] task, if FALSE returned tasks of this processor won't be even pulled from the DB
	 */
	protected boolean isReadyToHandleTasks() {
		return true;
	}

	/**
	 * allows implementations to check application readiness state per each specific task by custom application key
	 * - this API will be called each dispatch cycle AFTER the tasks were pulled from the DB, for each and every task 'in hand'
	 * - denying task will effectively leave it in queue without switching to RUNNING state
	 * - denying channeled task will effectively hold the whole channel (even if later tasks in the channel have different application key)
	 * - denying simple task will not have effect on other tasks beside the fact, that the order of execution will change, naturally
	 *
	 * @param applicationKey application key provided by consumer at enqueue time
	 * @return true if the application can run the task [default] or false to keep the task pending in queue
	 */
	protected boolean isTaskAbleToRun(String applicationKey) {
		return true;
	}

	//
	//  INTERNAL STUFF FROM HERE
	//
	final ClusterTasksDataProviderType getDataProviderType() {
		return dataProviderType;
	}

	final boolean isReadyToHandleTaskInternal() {
		boolean internalResult = true;
		if (availableWorkers.get() == 0) {
			internalResult = false;
		} else if (minimalTasksTakeInterval > 0) {
			internalResult = System.currentTimeMillis() - lastTaskHandledLocalTime > minimalTasksTakeInterval;
		}

		boolean foreignResult = true;
		if (internalResult) {
			Histogram.Timer foreignCallTimer = foreignIsReadyToHandleTasksCallDuration.labels(clusterTasksService.getInstanceID()).startTimer();
			foreignResult = isReadyToHandleTasks();
			foreignCallTimer.close();
		}
		return internalResult && foreignResult;
	}

	final Collection<ClusterTaskImpl> selectTasksToRun(List<ClusterTaskImpl> candidates) {
		Set<ClusterTaskImpl> tasksToRun = new HashSet<>();

		//  group tasks by concurrency key
		//  while filtering out tasks rejected on applicative per-task validation
		Map<String, List<ClusterTaskImpl>> tasksGroupedByConcurrencyKeys = new LinkedHashMap<>();
		for (ClusterTaskImpl candidate : candidates) {
			Histogram.Timer foreignCallTimer = foreignIsTaskAbleToRunCallDuration.labels(clusterTasksService.getInstanceID()).startTimer();
			boolean taskAbleToRan = isTaskAbleToRun(candidate.applicationKey);
			foreignCallTimer.close();
			if (taskAbleToRan) {
				String tmpCK = candidate.concurrencyKey != null ? candidate.concurrencyKey : NON_CONCURRENT_TASKS_GROUP_KEY;
				tasksGroupedByConcurrencyKeys
						.computeIfAbsent(tmpCK, ck -> new ArrayList<>())
						.add(candidate);
			}
		}

		if (!tasksGroupedByConcurrencyKeys.isEmpty()) {
			int availableWorkersTmp = availableWorkers.get();

			//  order relevant concurrency keys by fairness logic
			//  - first see the LRU concurrency key and give priority to it's channel
			//  - when two keys are equal, give priority to the channel with less ordered first item
			List<String> orderedRelevantKeys = new ArrayList<>(tasksGroupedByConcurrencyKeys.keySet());
			orderedRelevantKeys.sort((keyA, keyB) -> {
				long keyALastTouch = concurrencyKeysFairnessMap.getOrDefault(keyA, 0L);
				long keyBLastTouch = concurrencyKeysFairnessMap.getOrDefault(keyB, 0L);
				if (keyALastTouch != keyBLastTouch) {
					return Long.compare(keyALastTouch, keyBLastTouch);
				} else {
					ClusterTaskImpl headA = tasksGroupedByConcurrencyKeys.get(keyA).get(0);
					ClusterTaskImpl headB = tasksGroupedByConcurrencyKeys.get(keyB).get(0);
					int comp = Long.compare(headA.orderingFactor, headB.orderingFactor);
					return comp != 0 ? comp : Long.compare(headA.id, headB.id);
				}
			});

			//  first - select tasks fairly - including NON_CONCURRENT_TASKS_GROUP to let them chance to run as well
			//  here we taking a single (first) task from each CONCURRENT CHANNEL
			for (String concurrencyKey : orderedRelevantKeys) {
				if (availableWorkersTmp <= 0) break;
				List<ClusterTaskImpl> channeledTasksGroup = tasksGroupedByConcurrencyKeys.get(concurrencyKey);
				tasksToRun.add(channeledTasksGroup.get(0));
				availableWorkersTmp--;
			}

			//  second - if there are still available threads, give'em to the rest of the NON_CONCURRENT_TASKS_GROUP
			List<ClusterTaskImpl> nonConcurrentTasks = tasksGroupedByConcurrencyKeys.getOrDefault(NON_CONCURRENT_TASKS_GROUP_KEY, Collections.emptyList());
			for (ClusterTaskImpl task : nonConcurrentTasks) {
				if (availableWorkersTmp <= 0) break;
				tasksToRun.add(task);
				availableWorkersTmp--;
			}
		}

		return tasksToRun;
	}

	final void handleTasks(Collection<ClusterTaskImpl> tasks, ClusterTasksDataProvider dataProvider) {
		tasks.forEach(task -> {
			if (handoutTaskToWorker(dataProvider, task)) {
				concurrencyKeysFairnessMap.put(
						task.concurrencyKey != null ? task.concurrencyKey : NON_CONCURRENT_TASKS_GROUP_KEY,
						System.currentTimeMillis());
			} else {
				logger.error("failed to hand out " + task + " (task is already marked as RUNNING)");
			}
		});

		threadsUtilizationGauge
				.labels(getType())
				.set(((double) (numberOfWorkersPerNode - availableWorkers.get())) / ((double) numberOfWorkersPerNode));
	}

	final void notifyTaskWorkerFinished(ClusterTasksDataProvider dataProvider, ClusterTaskImpl task) {
		int aWorkers = availableWorkers.incrementAndGet();
		lastTaskHandledLocalTime = System.currentTimeMillis();
		logger.debug(type + " available workers " + aWorkers);

		//  submit task for removal
		clusterTasksService.getMaintainer().submitTaskToRemove(dataProvider, task);
	}

	private boolean handoutTaskToWorker(ClusterTasksDataProvider dataProvider, ClusterTaskImpl task) {
		try {
			ClusterTasksProcessorWorker worker = new ClusterTasksProcessorWorker(dataProvider, this, task);
			workersThreadPool.execute(worker);
			int aWorkers = availableWorkers.decrementAndGet();
			if (logger.isDebugEnabled()) {
				logger.debug("processor " + getType() + " took " + task);
				logger.debug(type + " available workers " + aWorkers);
			}
			return true;
		} catch (Exception e) {
			logger.error("processor " + getType() + " failed to take " + task, e);
			return false;
		}
	}

	private final class CTPWorkersThreadFactory implements ThreadFactory {

		@Override
		public Thread newThread(Runnable runnable) {
			Thread result = new Thread(runnable);
			result.setName("CTP " + runnable.getClass().getSimpleName() + " on behalf of " + type + "; TID: " + result.getId());
			result.setDaemon(true);
			return result;
		}
	}
}