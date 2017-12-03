package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorDefault;
import com.microfocus.octane.cluster.tasks.api.dto.TaskToProcess;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorFairness_test extends ClusterTasksProcessorDefault {
	public static List<String> keysProcessingEventLog = new LinkedList<>();

	protected ClusterTasksProcessorFairness_test() {
		super(ClusterTasksDataProviderType.DB, 1);
	}

	@Override
	public void processTask(TaskToProcess task) {
		keysProcessingEventLog.add(String.valueOf(task.getConcurrencyKey()));
	}
}
