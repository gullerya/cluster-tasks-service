package com.microfocus.cluster.tasks.simple;

import com.microfocus.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by gullery on 22/05/2019
 */

public class SimpleProcessorA_test extends ClusterTasksProcessorSimple {
	static final Map<String, Long> tasksProcessed = new LinkedHashMap<>();

	protected SimpleProcessorA_test() {
		super(ClusterTasksDataProviderType.DB, 1);
	}

	@Override
	public void processTask(ClusterTask task) {
		tasksProcessed.put(task.getBody(), System.currentTimeMillis());
	}
}
