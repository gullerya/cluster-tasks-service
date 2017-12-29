package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorDefault;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksHC_C_test extends ClusterTasksProcessorDefault {
	private static final Object COUNT_LOCK = new Object();
	public static final Map<Long, Long> taskIDs = new LinkedHashMap<>();
	public static volatile boolean count = false;

	protected ClusterTasksHC_C_test() {
		super(ClusterTasksDataProviderType.DB, 5);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (count) {
			synchronized (COUNT_LOCK) {
				if (taskIDs.containsKey(task.getId())) {
					System.out.println(System.currentTimeMillis() + " - " + task.getId() + " - " + Thread.currentThread().getId() + ", " + taskIDs.get(task.getId()));
				}
				taskIDs.put(task.getId(), Thread.currentThread().getId());
			}
		}
	}
}
