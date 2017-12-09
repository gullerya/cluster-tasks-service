package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorDefault;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorFairness_test_st extends ClusterTasksProcessorDefault {
	public static List<String> keysProcessingEventsLog = new LinkedList<>();
	public static List<Long> nonConcurrentEventsLog = new LinkedList<>();

	protected ClusterTasksProcessorFairness_test_st() {
		super(ClusterTasksDataProviderType.DB, 1);
	}

	@Override
	public void processTask(ClusterTask task) {
		keysProcessingEventsLog.add(String.valueOf(task.getConcurrencyKey()));
		if (task.getConcurrencyKey() == null) {
			nonConcurrentEventsLog.add(task.getOrderingFactor());
		}
	}
}
