package com.gullerya.cluster.tasks.processors;

import com.gullerya.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;

/**
 * Created by gullery on 21/11/2018
 */

public class ClusterTasksStaledTestA extends ClusterTasksProcessorSimple {
	public boolean drainMode = true;
	public boolean suspended = false;
	public boolean isStaleRole = true;
	public final Object MONITOR = new Object();
	public int takenTasksCounter = 0;

	protected ClusterTasksStaledTestA() {
		super(ClusterTasksDataProviderType.DB, 2);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (drainMode) return;

		synchronized (this) {
			takenTasksCounter++;
		}
		if (isStaleRole) {
			System.out.println("going to stale task");
			synchronized (MONITOR) {
				try {
					MONITOR.wait();
				} catch (InterruptedException ie) {
					System.out.println("interrupted while waiting");
				}
			}
			System.out.println("out of wait");
		} else {
			System.out.println("task taken");
		}
	}

	@Override
	protected boolean isReadyToHandleTasks() {
		return !suspended;
	}
}
