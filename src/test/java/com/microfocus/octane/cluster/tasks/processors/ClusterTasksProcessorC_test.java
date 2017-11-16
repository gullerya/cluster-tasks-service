package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.impl.ClusterTaskInternal;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorDefault;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorC_test extends ClusterTasksProcessorDefault {
	public static String BEAN_ID = "tasksProcessorC_test";

	public final Map<String, Timestamp> tasksProcessed = new LinkedHashMap<>();

	protected ClusterTasksProcessorC_test() {
		super(ClusterTasksDataProviderType.DB, 3);
	}

	@Override
	public void processTask(ClusterTaskInternal task) {
		tasksProcessed.put(task.getBody(), new Timestamp(System.currentTimeMillis()));
	}
}
