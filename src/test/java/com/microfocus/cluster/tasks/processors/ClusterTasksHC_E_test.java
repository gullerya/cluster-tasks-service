package com.microfocus.cluster.tasks.processors;

import com.microfocus.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import org.junit.Assert;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksHC_E_test extends ClusterTasksProcessorSimple {
	private static final Object COUNT_LOCK = new Object();
	public static final Map<Long, Long> taskIDs = new LinkedHashMap<>();
	public static volatile boolean count = false;

	protected ClusterTasksHC_E_test() {
		super(ClusterTasksDataProviderType.DB, 5);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (count) {
			Assert.assertEquals("some body to touch the body tables as well", task.getBody());
			synchronized (COUNT_LOCK) {
				if (taskIDs.containsKey(task.getId())) {
					System.out.println(System.currentTimeMillis() + " - " + task.getId() + " - " + Thread.currentThread().getId() + ", " + taskIDs.get(task.getId()));
				}
				taskIDs.put(task.getId(), Thread.currentThread().getId());
			}
		}
	}
}
