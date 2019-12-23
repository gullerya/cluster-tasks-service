package com.gullerya.cluster.tasks.processors;

import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.api.ClusterTasksProcessorSimple;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorFairnessTestST extends ClusterTasksProcessorSimple {
	public static List<String> keysProcessingEventsLog = new LinkedList<>();
	public static List<Long> nonConcurrentEventsLog = new LinkedList<>();

	protected ClusterTasksProcessorFairnessTestST() {
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
