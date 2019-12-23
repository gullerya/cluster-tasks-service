package com.gullerya.cluster.tasks.simple;

import com.gullerya.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by gullery on 22/05/2019
 */

public class SimpleProcessorATest extends ClusterTasksProcessorSimple {
	static final Map<String, Long> tasksProcessed = new LinkedHashMap<>();

	protected SimpleProcessorATest() {
		super(ClusterTasksDataProviderType.DB, 1);
	}

	@Override
	public void processTask(ClusterTask task) {
		tasksProcessed.put(task.getBody(), System.currentTimeMillis());
	}
}
