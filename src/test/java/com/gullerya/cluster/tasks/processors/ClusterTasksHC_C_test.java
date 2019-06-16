package com.gullerya.cluster.tasks.processors;

import com.gullerya.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksHC_C_test extends ClusterTasksProcessorSimple {
	private static final Object COUNT_LOCK = new Object();
	public static final String CONTENT = UUID.randomUUID().toString();
	public static final Map<Long, Long> taskIDs = new LinkedHashMap<>();
	public static volatile boolean count = false;

	protected ClusterTasksHC_C_test() {
		super(ClusterTasksDataProviderType.DB, 5);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (count) {
			if (!CONTENT.equals(task.getBody())) {
				System.out.println("'foreign' task slept in");
				return;
			}

			synchronized (COUNT_LOCK) {
				if (taskIDs.containsKey(task.getId())) {
					System.out.println(System.currentTimeMillis() + " - " + task.getId() + " - " + Thread.currentThread().getId() + ", " + taskIDs.get(task.getId()));
				}
				taskIDs.put(task.getId(), Thread.currentThread().getId());
			}
		}
	}
}
