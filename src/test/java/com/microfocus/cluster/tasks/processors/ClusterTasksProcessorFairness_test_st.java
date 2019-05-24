package com.microfocus.cluster.tasks.processors;

import com.microfocus.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorFairness_test_st extends ClusterTasksProcessorSimple {
	public static List<String> keysProcessingEventsLog = new LinkedList<>();
	public static List<Long> nonConcurrentEventsLog = new LinkedList<>();

	protected ClusterTasksProcessorFairness_test_st() {
		super(ClusterTasksDataProviderType.DB, 1);
	}

	@Override
	public void processTask(ClusterTask task) {
		System.out.println(task.getOrderingFactor() + " - " + task.getConcurrencyKey());
		keysProcessingEventsLog.add(String.valueOf(task.getConcurrencyKey()));
		if (task.getConcurrencyKey() == null) {
			nonConcurrentEventsLog.add(task.getOrderingFactor());
		}
	}
}
