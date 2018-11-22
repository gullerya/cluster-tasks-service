package com.microfocus.cluster.tasks.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

abstract class ClusterTasksInternalWorker implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Object HALT_MONITOR = new Object();
	private volatile CompletableFuture<Object> haltPromise;

	final ClusterTasksServiceImpl.SystemWorkersConfigurer configurer;

	ClusterTasksInternalWorker(ClusterTasksServiceImpl.SystemWorkersConfigurer configurer) {
		if (configurer == null) {
			throw new IllegalArgumentException("configurer MUST NOT be null");
		}
		this.configurer = configurer;
		Runtime.getRuntime().addShutdownHook(new Thread(this::halt, getClass().getSimpleName() + " shutdown listener"));
	}

	abstract void performWorkCycle();

	abstract Integer getEffectiveBreathingInterval();

	@Override
	public void run() {
		haltPromise = null;
		while (haltPromise == null) {
			if (configurer.isCTSServiceEnabled()) {
				performWorkCycle();
			}
			breathe();
		}
		haltPromise.complete(null);
	}

	CompletableFuture<Object> halt() {
		if (haltPromise == null) {
			System.out.println(getClass().getSimpleName() + " of " + configurer.getInstanceID() + " is halting");
			haltPromise = new CompletableFuture<>();
			synchronized (HALT_MONITOR) {
				HALT_MONITOR.notify();
			}
		}
		return haltPromise;
	}

	private void breathe() {
		try {
			Integer maintenanceInterval = getEffectiveBreathingInterval();
			synchronized (HALT_MONITOR) {
				HALT_MONITOR.wait(maintenanceInterval);
			}
		} catch (InterruptedException ie) {
			logger.warn("interrupted while breathing between dispatch rounds", ie);
		}
	}
}
