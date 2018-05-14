package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.errors.CtsSqlFailure;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by gullery on 27/05/2016.
 * <p>
 * Non-extensible, package protected, instance-less and stateless utils class that is responsible for SQLs compilation and resultSet mappings
 * Generated SQLs are DB type aware, while DB type is taken from the current context
 */

@Deprecated
final class ClusterTasksDbUtils {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksDbUtils.class);

	private ClusterTasksDbUtils() {
	}

	static List<TaskInternal> tasksMetadataReader(ResultSet resultSet) {
		List<TaskInternal> result = new LinkedList<>();
		TaskInternal tmpTask;
		Long tmpLong;
		String tmpString;
		try {
			while (resultSet.next()) {
				try {
					tmpTask = new TaskInternal();
					tmpTask.id = resultSet.getLong(ClusterTasksDbDataProvider.META_ID);
					tmpTask.taskType = ClusterTaskType.byValue(resultSet.getLong(ClusterTasksDbDataProvider.TASK_TYPE));
					tmpTask.processorType = resultSet.getString(ClusterTasksDbDataProvider.PROCESSOR_TYPE);
					tmpTask.uniquenessKey = resultSet.getString(ClusterTasksDbDataProvider.UNIQUENESS_KEY);
					tmpString = resultSet.getString(ClusterTasksDbDataProvider.CONCURRENCY_KEY);
					if (!resultSet.wasNull()) {
						tmpTask.concurrencyKey = tmpString;
					}
					tmpLong = resultSet.getLong(ClusterTasksDbDataProvider.ORDERING_FACTOR);
					if (!resultSet.wasNull()) {
						tmpTask.orderingFactor = tmpLong;
					}
					tmpTask.delayByMillis = resultSet.getLong(ClusterTasksDbDataProvider.DELAY_BY_MILLIS);
					tmpTask.maxTimeToRunMillis = resultSet.getLong(ClusterTasksDbDataProvider.MAX_TIME_TO_RUN);
					tmpLong = resultSet.getLong(ClusterTasksDbDataProvider.BODY_PARTITION);
					if (!resultSet.wasNull()) {
						tmpTask.partitionIndex = tmpLong;
					}

					result.add(tmpTask);
				} catch (Exception e) {
					logger.error("failed to read cluster task " + result.size(), e);
				}
			}
		} catch (SQLException sqle) {
			logger.error("failed to read cluster tasks", sqle);
			throw new CtsSqlFailure("failed to read cluster task", sqle);
		}
		return result;
	}

	static String rowToTaskBodyReader(ResultSet resultSet) {
		String result = null;
		try {
			if (resultSet.next()) {
				try {
					Clob clobBody = resultSet.getClob(ClusterTasksDbDataProvider.BODY);
					if (clobBody != null) {
						result = clobBody.getSubString(1, (int) clobBody.length());
					}
				} catch (SQLException sqle) {
					logger.error("failed to read cluster task body", sqle);
					throw new CtsSqlFailure("failed to read cluster task body", sqle);
				}
			}
		} catch (SQLException sqle) {
			logger.error("failed to find cluster task body", sqle);
			throw new CtsSqlFailure("failed to find cluster task body", sqle);
		}
		return result;
	}

	static List<TaskInternal> gcCandidatesReader(ResultSet resultSet) {
		List<TaskInternal> result = new LinkedList<>();
		try {
			while (resultSet.next()) {
				try {
					TaskInternal task = new TaskInternal();
					task.id = resultSet.getLong(ClusterTasksDbDataProvider.META_ID);
					task.taskType = ClusterTaskType.byValue(resultSet.getLong(ClusterTasksDbDataProvider.TASK_TYPE));
					Long tmpLong = resultSet.getLong(ClusterTasksDbDataProvider.BODY_PARTITION);
					if (!resultSet.wasNull()) {
						task.partitionIndex = tmpLong;
					}
					task.processorType = resultSet.getString(ClusterTasksDbDataProvider.PROCESSOR_TYPE);
					task.maxTimeToRunMillis = resultSet.getLong(ClusterTasksDbDataProvider.MAX_TIME_TO_RUN);
					result.add(task);
				} catch (SQLException sqle) {
					logger.error("failed to read cluster task body", sqle);
				}
			}
		} catch (SQLException sqle) {
			logger.error("failed to find cluster task body", sqle);
			throw new CtsSqlFailure("failed to find cluster task body", sqle);
		}
		return result;
	}

	static Map<String, Integer> scheduledPendingReader(ResultSet resultSet) {
		Map<String, Integer> result = new LinkedHashMap<>();
		try {
			while (resultSet.next()) {
				try {
					result.put(resultSet.getString(ClusterTasksDbDataProvider.PROCESSOR_TYPE), resultSet.getInt("total"));
				} catch (SQLException sqle) {
					logger.error("failed to read cluster task body", sqle);
				}
			}
		} catch (SQLException sqle) {
			logger.error("failed to find cluster task body", sqle);
			throw new CtsSqlFailure("failed to find cluster task body", sqle);
		}
		return result;
	}
}
