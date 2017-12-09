package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.ClusterTasksITUtils;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorDefault;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorUniqueness_test extends ClusterTasksProcessorDefault {
	public static boolean draining = true;
	public static List<String> bodies = new LinkedList<>();

	protected ClusterTasksProcessorUniqueness_test() {
		super(ClusterTasksDataProviderType.DB, 2);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (!draining) {
			ClusterTasksITUtils.sleepSafely(Long.parseLong(task.getBody()));
			bodies.add(task.getBody());
		}
	}
}
