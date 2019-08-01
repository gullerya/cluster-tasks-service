package com.gullerya.cluster.tasks.processors;

import com.gullerya.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.CTSTestsUtils;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorUniqueness_test extends ClusterTasksProcessorSimple {
	public static boolean draining = true;
	public static List<String> bodies = new LinkedList<>();

	protected ClusterTasksProcessorUniqueness_test() {
		super(ClusterTasksDataProviderType.DB, 2);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (!draining) {
			CTSTestsUtils.waitSafely(Long.parseLong(task.getBody()));
			bodies.add(task.getBody());
		}
	}
}
