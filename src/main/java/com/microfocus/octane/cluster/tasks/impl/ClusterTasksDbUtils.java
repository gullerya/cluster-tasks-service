package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.ClusterTaskStatus;
import com.microfocus.octane.cluster.tasks.api.CtsDBTypeNotSupported;
import com.microfocus.octane.cluster.tasks.api.CtsSqlFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI.DBType;

/**
 * Created by gullery on 27/05/2016.
 * <p>
 * Non-extensible, package protected, instance-less and stateless utils class that is responsible for SQLs compilation and resultSet mappings
 * Generated SQLs are DB type aware, while DB type is taken from the current context
 */

final class ClusterTasksDbUtils {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksDbUtils.class);

	//  Metadata table
	private static final String META_TABLE_NAME = "CLUSTER_TASK_META";
	private static final String META_COLUMNS_PREFIX = "CTSKM_";

	private static final String META_ID = META_COLUMNS_PREFIX.concat("ID");
	private static final String TASK_TYPE = META_COLUMNS_PREFIX.concat("TASK_TYPE");
	private static final String PROCESSOR_TYPE = META_COLUMNS_PREFIX.concat("PROCESSOR_TYPE");
	private static final String UNIQUENESS_KEY = META_COLUMNS_PREFIX.concat("UNIQUENESS_KEY");
	private static final String CONCURRENCY_KEY = META_COLUMNS_PREFIX.concat("CONCURRENCY_KEY");
	private static final String ORDERING_FACTOR = META_COLUMNS_PREFIX.concat("ORDERING_FACTOR");
	private static final String STATUS = META_COLUMNS_PREFIX.concat("STATUS");
	private static final String CREATED = META_COLUMNS_PREFIX.concat("CREATED");
	private static final String DELAY_BY_MILLIS = META_COLUMNS_PREFIX.concat("DELAY_BY_MILLIS");

	private static final String STARTED = META_COLUMNS_PREFIX.concat("STARTED");
	private static final String RUNTIME_INSTANCE = META_COLUMNS_PREFIX.concat("RUNTIME_INSTANCE");
	private static final String MAX_TIME_TO_RUN = META_COLUMNS_PREFIX.concat("MAX_TIME_TO_RUN");
	private static final String BODY_PARTITION = META_COLUMNS_PREFIX.concat("BODY_PARTITION");

	//  Content table
	private static final String BODY_TABLE_NAME = "CLUSTER_TASK_BODY_P";
	private static final String BODY_COLUMNS_PREFIX = "CTSKB_";
	private static final String BODY_ID = BODY_COLUMNS_PREFIX.concat("ID");
	private static final String BODY = BODY_COLUMNS_PREFIX.concat("BODY");

	private ClusterTasksDbUtils() {
	}

	//
	//  INSERT TASK
	//
	static String buildInsertTaskMetaSQL(DBType dbType) {
		String fields = String.join(",",
				META_ID,
				TASK_TYPE,
				PROCESSOR_TYPE,
				UNIQUENESS_KEY,
				CONCURRENCY_KEY,
				DELAY_BY_MILLIS,
				MAX_TIME_TO_RUN,
				BODY_PARTITION,
				ORDERING_FACTOR,
				CREATED,
				STATUS);
		if (DBType.ORACLE == dbType) {
			return "INSERT INTO " + META_TABLE_NAME + " (" + fields + ") " +
					"VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, TO_NUMBER(TO_CHAR(SYSTIMESTAMP,'yyyymmddhh24missff3')) + ?), SYSDATE, " + ClusterTaskStatus.PENDING.value + ")";
		} else if (DBType.MSSQL == dbType) {
			return "INSERT INTO " + META_TABLE_NAME + " (" + fields + ") " +
					"VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CAST(FORMAT(CURRENT_TIMESTAMP,'yyyyMMddHHmmssfff') AS BIGINT) + ?), GETDATE(), " + ClusterTaskStatus.PENDING.value + ")";
		} else {
			throw new CtsDBTypeNotSupported("DB type " + dbType + " is not supported");
		}
	}

	static String buildInsertTaskBodySQL(Long partitionIndex) {
		if (partitionIndex == null) {
			throw new IllegalArgumentException("partition index MUST NOT be null");
		}
		return "INSERT INTO " + BODY_TABLE_NAME + partitionIndex + " (" + String.join(",", BODY_ID, BODY) + ") VALUES (?, ?)";
	}

	//
	//  PROCESS TASKS - including select, update running and retrieve content
	//
	static String buildSelectForUpdateTasksSQL(DBType dbType, int maxProcessorTypes) {
		String selectFields = String.join(",", META_ID, TASK_TYPE, PROCESSOR_TYPE, UNIQUENESS_KEY, CONCURRENCY_KEY, ORDERING_FACTOR, BODY_PARTITION);
		String processorTypesInParameter = String.join(",", Collections.nCopies(maxProcessorTypes, "?"));

		if (DBType.ORACLE == dbType) {
			return "SELECT " + selectFields +
					" FROM " + META_TABLE_NAME + " WHERE ROWID IN " +
					"   (SELECT row_id FROM" +
					"       (SELECT ROWID AS row_id," +
					"               ROW_NUMBER() OVER (PARTITION BY COALESCE(" + CONCURRENCY_KEY + ",RAWTOHEX(SYS_GUID())) ORDER BY " + ORDERING_FACTOR + "," + CREATED + "," + META_ID + " ASC) AS row_index," +
					"               COUNT(CASE WHEN " + STATUS + " = " + ClusterTaskStatus.RUNNING.value + " THEN 1 ELSE NULL END) OVER (PARTITION BY COALESCE(" + CONCURRENCY_KEY + ",RAWTOHEX(SYS_GUID()))) AS running_count" +
					"       FROM " + META_TABLE_NAME +
					"       WHERE " + PROCESSOR_TYPE + " IN(" + processorTypesInParameter + ")" +
					"           AND " + STATUS + " < " + ClusterTaskStatus.FINISHED.value +
					"           AND " + CREATED + " <= SYSDATE - NUMTODSINTERVAL(" + DELAY_BY_MILLIS + " / 1000, 'SECOND')) meta" +
					"   WHERE meta.row_index <= 1 AND meta.running_count = 0)" +
					"   ORDER BY " + ORDERING_FACTOR +
					" FOR UPDATE";
		} else if (DBType.MSSQL == dbType) {
			return "SELECT " + selectFields +
					" FROM " + META_TABLE_NAME + " WITH (UPDLOCK) WHERE " + META_ID + " IN " +
					"   (SELECT " + META_ID + " FROM" +
					"       (SELECT " + META_ID + "," +
					"               ROW_NUMBER() OVER (PARTITION BY COALESCE(" + CONCURRENCY_KEY + ",CAST(NEWID() AS VARCHAR(64))) ORDER BY " + ORDERING_FACTOR + "," + CREATED + "," + META_ID + " ASC) AS row_index," +
					"               COUNT(CASE WHEN " + STATUS + " = " + ClusterTaskStatus.RUNNING.value + " THEN 1 ELSE NULL END) OVER (PARTITION BY COALESCE(" + CONCURRENCY_KEY + ",CAST(NEWID() AS VARCHAR(64)))) AS running_count" +
					"       FROM " + META_TABLE_NAME +
					"       WHERE " + PROCESSOR_TYPE + " IN(" + processorTypesInParameter + ")" +
					"           AND " + STATUS + " < " + ClusterTaskStatus.FINISHED.value +
					"           AND " + CREATED + " <= DATEADD(MILLISECOND, -" + DELAY_BY_MILLIS + ", GETDATE())) meta" +
					"   WHERE meta.row_index <= 1 AND meta.running_count = 0)" +
					"   ORDER BY " + ORDERING_FACTOR;
		} else {
			throw new CtsDBTypeNotSupported("DB type " + dbType + " is not supported");
		}
	}

	static String buildUpdateTaskStartedSQL(DBType dbType, int numberOfTasks) {
		String inParameter = String.join(",", Collections.nCopies(numberOfTasks, "?"));
		if (DBType.ORACLE == dbType) {
			return "UPDATE " + META_TABLE_NAME + " SET " +
					STATUS + " = " + ClusterTaskStatus.RUNNING.value + ", " +
					STARTED + " = SYSDATE, " +
					RUNTIME_INSTANCE + " = ? WHERE " + META_ID + " IN (" + inParameter + ")";
		} else if (DBType.MSSQL == dbType) {
			return "UPDATE " + META_TABLE_NAME + " SET " +
					STATUS + " = " + ClusterTaskStatus.RUNNING.value + ", " +
					STARTED + " = GETDATE(), " +
					RUNTIME_INSTANCE + " = ? WHERE " + META_ID + " IN (" + inParameter + ")";
		} else {
			throw new CtsDBTypeNotSupported("DB type " + dbType + " is not supported");
		}
	}

	static String buildReadTaskBodySQL(Long partitionIndex) {
		if (partitionIndex == null) {
			throw new IllegalArgumentException("partition index MUST NOT be null");
		}
		return "SELECT " + BODY + " FROM " + BODY_TABLE_NAME + partitionIndex + " WHERE " + BODY_ID + " = ?";
	}

	static String buildUpdateTaskFinishedSQL() {
		return "UPDATE " + META_TABLE_NAME + " SET " + STATUS + " = " + ClusterTaskStatus.FINISHED.value + " WHERE " + META_ID + " = ?";
	}

	static String buildUpdateTaskReenqueueSQL(int numberOfTasks) {
		String inParameter = String.join(",", Collections.nCopies(numberOfTasks, "?"));
		return "UPDATE " + META_TABLE_NAME +
				" SET " + STATUS + " = " + ClusterTaskStatus.PENDING.value +
				", " + STARTED + " = NULL " +
				", " + RUNTIME_INSTANCE + " = NULL " +
				" WHERE " + META_ID + " IN (" + inParameter + ")";
	}

	//
	//  DELETE TASKS - garbage collection flow
	//
	static String buildSelectGCValidTasksSQL(DBType dbType) {
		String selectedFields = String.join(",", META_ID, BODY_PARTITION, TASK_TYPE);
		if (DBType.ORACLE == dbType) {
			return "SELECT " + selectedFields + " FROM " + META_TABLE_NAME +
					" WHERE " + STATUS + " = " + ClusterTaskStatus.FINISHED.value +
					" OR (" + STATUS + " = " + ClusterTaskStatus.RUNNING.value + " AND " + STARTED + " < SYSDATE - NUMTODSINTERVAL(" + MAX_TIME_TO_RUN + " / 1000, 'SECOND'))";
		} else if (DBType.MSSQL == dbType) {
			return "SELECT " + selectedFields + " FROM " + META_TABLE_NAME +
					" WHERE " + STATUS + " = " + ClusterTaskStatus.FINISHED.value +
					" OR (" + STATUS + " = " + ClusterTaskStatus.RUNNING.value + " AND DATEDIFF(MILLISECOND, " + STARTED + ", GETDATE()) > " + MAX_TIME_TO_RUN + ")";
		} else {
			throw new CtsDBTypeNotSupported("DB type " + dbType + " is not supported");
		}
	}

	static String buildDeleteTaskMetaSQL(int deleteBulkSize) {
		String inParam = String.join(",", Collections.nCopies(deleteBulkSize, "?"));
		return "DELETE FROM " + META_TABLE_NAME + " WHERE " + META_ID + " IN (" + inParam + ")";
	}

	static String buildDeleteTaskBodySQL(Long partitionIndex, int deleteBulkSize) {
		if (partitionIndex == null) {
			throw new IllegalArgumentException("partition index MUST NOT be null");
		}
		String inParam = String.join(",", Collections.nCopies(deleteBulkSize, "?"));
		return "DELETE FROM " + BODY_TABLE_NAME + partitionIndex + " WHERE " + BODY_ID + " IN (" + inParam + ")";
	}

	static String buildSelectVerifyBodyTableSQL(Long partitionIndex) {
		if (partitionIndex == null) {
			throw new IllegalArgumentException("partition index MUST NOT be null");
		}
		String fieldsToSelect = String.join(",", BODY_ID, BODY, META_ID);
		return "SELECT " + fieldsToSelect + " FROM " + BODY_TABLE_NAME + partitionIndex +
				" LEFT OUTER JOIN " + META_TABLE_NAME + " ON " + META_ID + " = " + BODY_ID;
	}

	static String buildTruncateBodyTableSQL(Long partitionIndex) {
		if (partitionIndex == null) {
			throw new IllegalArgumentException("partition index MUST NOT be null");
		}
		return "TRUNCATE TABLE " + BODY_TABLE_NAME + partitionIndex;
	}

	//
	//  COUNT - tasks counting
	//
	static String buildCountTasksSQL(String processorType, Set<ClusterTaskStatus> statuses) {
		List<String> queryClauses = new LinkedList<>();
		if (processorType != null) {
			queryClauses.add(PROCESSOR_TYPE + " = '" + processorType + "'");
		}
		if (statuses != null && !statuses.isEmpty()) {
			queryClauses.add(STATUS + " IN (" + String.join(",", statuses.stream().map(status -> String.valueOf(status.value)).collect(Collectors.toList())) + ")");
		}

		return buildCountSQLByQueries(queryClauses);
	}

	static String buildCountTasksSQL(String processorType, String concurrencyKey, Set<ClusterTaskStatus> statuses) {
		List<String> queryClauses = new LinkedList<>();
		if (processorType != null) {
			queryClauses.add(PROCESSOR_TYPE + " = '" + processorType + "'");
		}
		if (concurrencyKey != null) {
			queryClauses.add(CONCURRENCY_KEY + " = '" + concurrencyKey + "'");
		} else {
			queryClauses.add(CONCURRENCY_KEY + " IS NULL");
		}
		if (statuses != null && !statuses.isEmpty()) {
			queryClauses.add(STATUS + " IN (" + String.join(",", statuses.stream().map(status -> String.valueOf(status.value)).collect(Collectors.toList())) + ")");
		}

		return buildCountSQLByQueries(queryClauses);
	}

	//
	//  READERS - DB responses processors
	//
	static List<ClusterTask> tasksMetadataReader(ResultSet resultSet) {
		List<ClusterTask> result = new LinkedList<>();
		ClusterTask tmpTask;
		Long tmpLong;
		String tmpString;
		try {
			while (resultSet.next()) {
				try {
					tmpTask = new ClusterTask(resultSet.getLong(META_ID));
					tmpTask.setTaskType(ClusterTaskType.byValue(resultSet.getLong(TASK_TYPE)));
					tmpTask.setProcessorType(resultSet.getString(PROCESSOR_TYPE));
					tmpTask.setUniquenessKey(resultSet.getString(UNIQUENESS_KEY));
					tmpString = resultSet.getString(CONCURRENCY_KEY);
					if (!resultSet.wasNull()) {
						tmpTask.setConcurrencyKey(tmpString);
					}
					tmpLong = resultSet.getLong(ORDERING_FACTOR);
					if (!resultSet.wasNull()) {
						tmpTask.setOrderingFactor(tmpLong);
					}
					tmpLong = resultSet.getLong(BODY_PARTITION);
					if (!resultSet.wasNull()) {
						tmpTask.setPartitionIndex(tmpLong);
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
					Clob clobBody = resultSet.getClob(BODY);
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

	static List<ClusterTask> gcCandidatesReader(ResultSet resultSet) {
		List<ClusterTask> result = new LinkedList<>();
		try {
			while (resultSet.next()) {
				try {
					ClusterTask task = new ClusterTask(resultSet.getLong(META_ID));
					task.setTaskType(ClusterTaskType.byValue(resultSet.getLong(TASK_TYPE)));
					task.setPartitionIndex(resultSet.getLong(BODY_PARTITION));
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

	static BodyTablePreTruncateVerificationResult rowsToIDsInPartitionReader(ResultSet resultSet) {
		BodyTablePreTruncateVerificationResult result = new BodyTablePreTruncateVerificationResult();
		try {
			while (resultSet.next()) {
				try {
					Long bodyId;
					String body;
					Long metaId;

					bodyId = resultSet.getLong(BODY_ID);
					if (resultSet.wasNull()) {
						bodyId = null;
					}
					Clob clobBody = resultSet.getClob(BODY);
					if (!resultSet.wasNull() && clobBody != null) {
						body = clobBody.getSubString(1, (int) clobBody.length());
					} else {
						body = null;
					}
					metaId = resultSet.getLong(META_ID);
					if (resultSet.wasNull()) {
						metaId = null;
					}

					result.addEntry(bodyId, body, metaId);
				} catch (SQLException sqle) {
					logger.error("failed to read cluster task (body ID)", sqle);
				}
			}
		} catch (SQLException sqle) {
			logger.error("failed to perform empty body table data collection", sqle);
		}
		return result;
	}

	private static String buildCountSQLByQueries(List<String> queryClauses) {
		return "SELECT COUNT(*) FROM " + META_TABLE_NAME +
				(queryClauses.isEmpty() ? "" : " WHERE " + String.join(" AND ", queryClauses));
	}
}
