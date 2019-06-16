package com.gullerya.cluster.tasks.applicationkey;

import com.gullerya.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.CTSTestsUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by gullery on 22/05/2019
 */

public class AppKeyProcessorCount_test extends ClusterTasksProcessorSimple {
	static final Map<Long, Long> tasksProcessed = new LinkedHashMap<>();
	static volatile boolean any = false;
	static volatile boolean holdRunning = false;
	static volatile String conditionToRun = null;

	protected AppKeyProcessorCount_test() {
		super(ClusterTasksDataProviderType.DB, 3);
	}

	@Override
	protected boolean isTaskAbleToRun(String applicationKey) {
		return any || Objects.equals(conditionToRun, applicationKey);
	}

	@Override
	public void processTask(ClusterTask task) {
		CTSTestsUtils.waitUntil(10000, () -> holdRunning ? null : true);
		synchronized (tasksProcessed) {
			tasksProcessed.put(task.getId(), System.currentTimeMillis());
		}
	}
}
