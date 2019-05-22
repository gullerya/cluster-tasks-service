package com.microfocus.cluster.tasks.applicationkey;

import com.microfocus.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by gullery on 22/05/2019
 */

public class AppKeyProcessorB_test extends ClusterTasksProcessorSimple {
	static final Map<String, Long> tasksProcessed = new LinkedHashMap<>();
	static volatile String conditionToRun = null;

	protected AppKeyProcessorB_test() {
		super(ClusterTasksDataProviderType.DB, 1);
	}

	@Override
	protected boolean isTaskAbleToRun(String applicationKey) {
		return Objects.equals(conditionToRun, applicationKey);
	}

	@Override
	public void processTask(ClusterTask task) {
		tasksProcessed.put(task.getBody(), System.currentTimeMillis());
	}
}
