package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorBasic;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorFairness_test_mt extends ClusterTasksProcessorBasic {
	public static final List<String> keysProcessingEventsLog = new LinkedList<>();

	protected ClusterTasksProcessorFairness_test_mt() {
		super(ClusterTasksDataProviderType.DB, 4);
	}

	@Override
	public void processTask(ClusterTask task) {
		synchronized (keysProcessingEventsLog) {
			keysProcessingEventsLog.add(String.valueOf(task.getConcurrencyKey()));
		}
	}
}
