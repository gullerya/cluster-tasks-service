package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksService;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by gullery on 12/04/2018.
 *
 * PostgreSQL oriented data provider
 */

final class PostgreSqlDbDataProvider extends ClusterTasksDbDataProvider {
	private static final Logger logger = LoggerFactory.getLogger(PostgreSqlDbDataProvider.class);

	PostgreSqlDbDataProvider(ClusterTasksService clusterTasksService, ClusterTasksServiceConfigurerSPI serviceConfigurer) {
		super(clusterTasksService, serviceConfigurer);
	}

	@Override
	boolean isReady() {
		return false;
	}

	//  TODO: support bulk insert here
	@Override
	ClusterTaskPersistenceResult[] storeTasks(TaskInternal... tasks) {
		return null;
	}

	@Override
	void retrieveAndDispatchTasks(Map<String, ClusterTasksProcessorBase> availableProcessors) {
	}

	@Override
	String retrieveTaskBody(Long taskId, Long partitionIndex) {
		return null;
	}

	@Override
	void updateTaskToFinished(Long taskId) {
	}

	@Override
	void handleGarbageAndStaled() {
	}

	@Override
	void reinsertScheduledTasks(List<TaskInternal> candidatesToReschedule) {
	}

	private String buildInsertTaskSQL(Long partitionIndex) {
		return null;
	}

	private String buildSelectForUpdateTasksSQL(int maxProcessorTypes) {
		return null;
	}

	private String buildUpdateTaskStartedSQL() {
		return null;
	}

	private String buildUpdateTaskFinishedSQL() {
		return null;
	}

	private String buildCountScheduledPendingTasksSQL() {
		return null;
	}

	private String buildSelectGCValidTasksSQL() {
		return null;
	}
}