package com.gullerya.cluster.tasks.applicationkey;

import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.api.ClusterTasksProcessorSimple;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by gullery on 22/05/2019
 */

public class AppKeyProcessorB_test extends ClusterTasksProcessorSimple {
	static final Map<String, Long> tasksProcessed = new LinkedHashMap<>();
	static volatile boolean any = true;
	static volatile String conditionToRun = null;

	protected AppKeyProcessorB_test() {
		super(ClusterTasksDataProviderType.DB, 1);
	}

	@Override
	protected boolean isTaskAbleToRun(String applicationKey) {
		return any || Objects.equals(conditionToRun, applicationKey);
	}

	@Override
	public void processTask(ClusterTask task) {
		tasksProcessed.put(task.getBody(), System.currentTimeMillis());
	}
}
