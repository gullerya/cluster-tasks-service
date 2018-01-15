package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorSimple;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorF_test_cna extends ClusterTasksProcessorSimple {
	public final Map<String, String> tasksProcessed = new LinkedHashMap<>();

	protected ClusterTasksProcessorF_test_cna() {
		super(ClusterTasksDataProviderType.DB, 1);
	}

	@Override
	public void processTask(ClusterTask task) {
		tasksProcessed.put(task.getBody(), task.getBody());
	}
}
