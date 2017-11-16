package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.impl.ClusterTaskInternal;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorDefault;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorF_test_cna extends ClusterTasksProcessorDefault {
	public static String BEAN_ID = "tasksProcessorF_test_concurrent_with_non_available";

	public final Map<String, String> tasksProcessed = new LinkedHashMap<>();

	protected ClusterTasksProcessorF_test_cna() {
		super(ClusterTasksDataProviderType.DB, 1);
	}

	@Override
	public void processTask(ClusterTaskInternal task) {
		tasksProcessed.put(task.getBody(), task.getBody());
	}
}
