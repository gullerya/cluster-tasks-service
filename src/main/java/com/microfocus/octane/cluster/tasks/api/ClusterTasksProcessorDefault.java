package com.microfocus.octane.cluster.tasks.api;

import com.microfocus.octane.cluster.tasks.impl.ClusterTasksWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gullery on 08/05/2016.
 * <p>
 * API definition and base implementation of DEFAULT/REGULAR Cluster Tasks Processor
 */

public abstract class ClusterTasksProcessorDefault {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksProcessorDefault.class);

	private final String type;
	private final ClusterTasksDataProviderType dataProviderType;
	private int numberOfWorkersPerNode;
	private int minimalTasksTakeInterval;
	private long lastTaskHandledLocalTime;
	private ExecutorService workersThreadPool;
	private AtomicInteger availableWorkers = new AtomicInteger(0);

	protected ClusterTasksProcessorDefault(ClusterTasksDataProviderType dataProviderType, int numberOfWorkersPerNode) {
		this(dataProviderType, numberOfWorkersPerNode, 0);
	}

	protected ClusterTasksProcessorDefault(ClusterTasksDataProviderType dataProviderType, int numberOfWorkersPerNode, int minimalTasksTakeInterval) {
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

	public String getType() {
		return type;
	}

	public ClusterTasksDataProviderType getDataProviderType() {
		return dataProviderType;
	}

	abstract public void processTask(ClusterTask task) throws Exception;

	protected void setMinimalTasksTakeInterval(Integer minimalTasksTakeInterval) {
		minimalTasksTakeInterval = minimalTasksTakeInterval == null ? ClusterTasksServiceConfigurerSPI.DEFAULT_POLL_INTERVAL : minimalTasksTakeInterval;
		this.minimalTasksTakeInterval = Math.max(minimalTasksTakeInterval, ClusterTasksServiceConfigurerSPI.MINIMAL_POLL_INTERVAL);
	}

	protected boolean isReadyToHandleTask() {
		return true;
	}

	//  INTERNAL - to be invoked by CTS system only
	public final boolean isReadyToHandleTaskInternal() {
		boolean internalResult = true;
		if (availableWorkers.get() == 0) {
			internalResult = false;
		} else if (minimalTasksTakeInterval > 0) {
			internalResult = System.currentTimeMillis() - lastTaskHandledLocalTime > minimalTasksTakeInterval;
		}
		return internalResult && isReadyToHandleTask();
	}

	//  INTERNAL - to be invoked by CTS system only
	public void internalProcessTasksAsync(ClusterTasksWorker worker) {
		notifyTaskWorkerStarted();
		workersThreadPool.execute(worker);
	}

	private void notifyTaskWorkerStarted() {
		int aWorkers = availableWorkers.decrementAndGet();
		logger.debug(type + " available workers " + aWorkers);
	}

	public void notifyTaskWorkerFinished() {
		int aWorkers = availableWorkers.incrementAndGet();
		lastTaskHandledLocalTime = System.currentTimeMillis();
		logger.debug(type + " available workers " + aWorkers);
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