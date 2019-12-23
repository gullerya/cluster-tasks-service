package com.gullerya.cluster.tasks.processors;

import com.gullerya.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorFairnessTestMT extends ClusterTasksProcessorSimple {
	public static final List<String> keysProcessingEventsLog = new LinkedList<>();

	protected ClusterTasksProcessorFairnessTestMT() {
		super(ClusterTasksDataProviderType.DB, 4);
	}

	@Override
	public void processTask(ClusterTask task) {
		synchronized (keysProcessingEventsLog) {
			keysProcessingEventsLog.add(String.valueOf(task.getConcurrencyKey()));
		}
	}
}
