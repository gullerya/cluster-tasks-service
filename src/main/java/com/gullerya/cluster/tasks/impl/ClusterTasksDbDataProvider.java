/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.gullerya.cluster.tasks.impl;

import com.gullerya.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.gullerya.cluster.tasks.api.enums.ClusterTaskInsertStatus;
import com.gullerya.cluster.tasks.api.enums.ClusterTaskStatus;
import com.gullerya.cluster.tasks.api.enums.ClusterTaskType;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.api.errors.CtsGeneralFailure;
import com.gullerya.cluster.tasks.api.ClusterTasksService;
import com.gullerya.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.gullerya.cluster.tasks.api.errors.CtsSqlFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

/**
 * Created by gullery on 08/05/2016.
 * <p>
 * Cluster tasks data provider backed by DB
 */

abstract class ClusterTasksDbDataProvider implements ClusterTasksDataProvider {
	private final Logger logger = LoggerFactory.getLogger(ClusterTasksDbDataProvider.class);

	protected final ClusterTasksService clusterTasksService;
	private final ClusterTasksServiceConfigurerSPI serviceConfigurer;

	volatile Boolean isReady = null;

	//  Active nodes table
	static final String ACTIVE_NODES_TABLE_NAME = "CTS_ACTIVE_NODES";
	static final String ACTIVE_NODE_ID = "CTSAN_NODE_ID";
	static final String ACTIVE_NODE_SINCE = "CTSAN_SINCE";
	static final String ACTIVE_NODE_LAST_SEEN = "CTSAN_LAST_SEEN";

	//  Metadata table
	private static final String META_COLUMNS_PREFIX = "CTSKM_";
	static final String META_TABLE_NAME = "CLUSTER_TASK_META";
	static final String CLUSTER_TASK_ID_SEQUENCE = "CLUSTER_TASK_ID";
	static final String META_ID = META_COLUMNS_PREFIX.concat("ID");
	static final String TASK_TYPE = META_COLUMNS_PREFIX.concat("TASK_TYPE");
	static final String PROCESSOR_TYPE = META_COLUMNS_PREFIX.concat("PROCESSOR_TYPE");
	static final String UNIQUENESS_KEY = META_COLUMNS_PREFIX.concat("UNIQUENESS_KEY");
	static final String CONCURRENCY_KEY = META_COLUMNS_PREFIX.concat("CONCURRENCY_KEY");
	static final String APPLICATION_KEY = META_COLUMNS_PREFIX.concat("APPLICATION_KEY");
	static final String ORDERING_FACTOR = META_COLUMNS_PREFIX.concat("ORDERING_FACTOR");
	static final String STATUS = META_COLUMNS_PREFIX.concat("STATUS");
	static final String CREATED = META_COLUMNS_PREFIX.concat("CREATED");
	static final String DELAY_BY_MILLIS = META_COLUMNS_PREFIX.concat("DELAY_BY_MILLIS");

	static final String STARTED = META_COLUMNS_PREFIX.concat("STARTED");
	static final String RUNTIME_INSTANCE = META_COLUMNS_PREFIX.concat("RUNTIME_INSTANCE");
	static final String BODY_PARTITION = META_COLUMNS_PREFIX.concat("BODY_PARTITION");

	//  Content table
	private static final String BODY_COLUMNS_PREFIX = "CTSKB_";
	static final String BODY_TABLE_NAME = "CLUSTER_TASK_BODY_P";
	static final String BODY_ID = BODY_COLUMNS_PREFIX.concat("ID");
	static final String BODY = BODY_COLUMNS_PREFIX.concat("BODY");

	final int PARTITIONS_NUMBER = 4;

	private final String removeFinishedTaskSQL;
	private final String removeFinishedTasksByQuerySQL;
	private final Map<Long, String> selectDanglingBodiesSQLs = new HashMap<>();
	private final Map<Long, String> removeDanglingBodiesSQLs = new HashMap<>();
	private final int removeDanglingBodiesBulkSize = 50;

	private final String removeStaledTasksSQL;

	private final String countScheduledPendingTasksSQL;

	private final Map<Long, String> lookupOrphansByPartitionSQLs = new LinkedHashMap<>();
	private final Map<Long, String> truncateByPartitionSQLs = new LinkedHashMap<>();

	private final String countTasksByStatusSQL;
	private final Map<Long, String> countTaskBodiesByPartitionSQLs = new LinkedHashMap<>();

	private ZonedDateTime lastTruncateTime;
	private JdbcTemplate jdbcTemplate;
	private TransactionTemplate transactionTemplate;

	ClusterTasksDbDataProvider(ClusterTasksService clusterTasksService, ClusterTasksServiceConfigurerSPI serviceConfigurer) {
		this.clusterTasksService = clusterTasksService;
		this.serviceConfigurer = serviceConfigurer;

		//  prepare SQL statements
		removeFinishedTaskSQL = "DELETE FROM " + META_TABLE_NAME + " WHERE " + META_ID + " = ?";
		removeFinishedTasksByQuerySQL = "DELETE FROM " + META_TABLE_NAME + " WHERE " + STATUS + " = " + ClusterTaskStatus.FINISHED.value;

		for (long partition = 0; partition < PARTITIONS_NUMBER; partition++) {
			lookupOrphansByPartitionSQLs.put(partition, "SELECT " + String.join(",", BODY_ID, META_ID) + " FROM " + BODY_TABLE_NAME + partition +
					" LEFT OUTER JOIN " + META_TABLE_NAME + " ON " + META_ID + " = " + BODY_ID);

			selectDanglingBodiesSQLs.put(partition, "SELECT " + BODY_ID + " AS bodyId FROM " + BODY_TABLE_NAME + partition +
					" WHERE NOT EXISTS (SELECT 1 FROM " + META_TABLE_NAME + " WHERE " + META_ID + " = " + BODY_ID + ")");
			removeDanglingBodiesSQLs.put(partition, "DELETE FROM " + BODY_TABLE_NAME + partition + " WHERE " + BODY_ID + " IN (" + String.join(",", Collections.nCopies(removeDanglingBodiesBulkSize, "?")) + ")");

			truncateByPartitionSQLs.put(partition, "TRUNCATE TABLE " + BODY_TABLE_NAME + partition);
			countTaskBodiesByPartitionSQLs.put(partition, "SELECT COUNT(*) AS counter FROM " + BODY_TABLE_NAME + partition);
		}

		removeStaledTasksSQL = "DELETE FROM " + META_TABLE_NAME +
				" WHERE " + RUNTIME_INSTANCE + " IS NOT NULL" +
				"   AND NOT EXISTS (SELECT 1 FROM " + ACTIVE_NODES_TABLE_NAME + " WHERE " + ACTIVE_NODE_ID + " = " + RUNTIME_INSTANCE + ")";

		countScheduledPendingTasksSQL = "SELECT " + PROCESSOR_TYPE + ",COUNT(*) AS total FROM " + META_TABLE_NAME +
				" WHERE " + TASK_TYPE + " = " + ClusterTaskType.SCHEDULED.value + " AND " + STATUS + " = " + ClusterTaskStatus.PENDING.value + " GROUP BY " + PROCESSOR_TYPE;

		countTasksByStatusSQL = "SELECT COUNT(*) AS counter," + PROCESSOR_TYPE + " FROM " + META_TABLE_NAME + " WHERE " + STATUS + " = ? GROUP BY " + PROCESSOR_TYPE;
	}

	abstract String[] getSelectReRunnableStaledTasksSQL();

	abstract String getUpdateScheduledTaskIntervalSQL();

	@Override
	public ClusterTasksDataProviderType getType() {
		return ClusterTasksDataProviderType.DB;
	}

	@Override
	public void updateScheduledTaskInterval(String scheduledTaskType, long newTaskRunInterval) {
		String sql = getUpdateScheduledTaskIntervalSQL();
		JdbcTemplate jdbcTemplate = getJdbcTemplate();

		boolean done = CTSUtils.retry(6, () -> {
			int updatedEntries = jdbcTemplate.update(sql, new Object[]{newTaskRunInterval, scheduledTaskType}, new int[]{Types.BIGINT, Types.VARCHAR});
			if (updatedEntries == 1) {
				logger.info("successfully updated scheduled task " + scheduledTaskType + " to a new interval " + newTaskRunInterval);
				return true;
			} else {
				logger.warn("unexpectedly got " + updatedEntries + " updated entries count while expected for 1 (updating " + scheduledTaskType + " with new interval " + newTaskRunInterval + "); won't retry");
				return false;
			}
		});

		if (!done) {
			logger.error("finally failed to update interval of scheduled task " + scheduledTaskType);
		}
	}

	@Override
	public boolean removeTaskById(Long taskId) {
		int removed = getJdbcTemplate().update(removeFinishedTaskSQL, new Object[]{taskId}, new int[]{Types.BIGINT});
		return removed == 1;
	}

	@Override
	public void removeFinishedTasksByQuery() {
		try {
			int affected = getJdbcTemplate().update(removeFinishedTasksByQuerySQL);
			if (affected > 0) {
				logger.debug("removed " + affected + " finished tasks by query");
			}
		} catch (DataAccessException dae) {
			logger.error("failed during removal of finished tasks by query", dae);
		}
	}

	@Override
	public void cleanFinishedTaskBodiesByIDs(long partitionIndex, Long[] taskBodies) {
		try {
			int index = 0;
			Object[] params = new Object[removeDanglingBodiesBulkSize];
			int[] types = new int[removeDanglingBodiesBulkSize];
			for (int i = 0; i < removeDanglingBodiesBulkSize; i++) types[i] = Types.BIGINT;
			while (index < taskBodies.length) {
				System.arraycopy(taskBodies, index, params, 0, Math.min(taskBodies.length - index, removeDanglingBodiesBulkSize));
				int removed = getJdbcTemplate().update(removeDanglingBodiesSQLs.get(partitionIndex), params, types);
				logger.debug("removed " + removed + " bodies from partition " + partitionIndex);
				index += removeDanglingBodiesBulkSize;
			}
		} catch (DataAccessException dae) {
			logger.error("failed during cleaning dangling task bodies", dae);
		}
	}

	@Override
	public void removeFinishedTaskBodiesByQuery() {
		long partitionIndex = resolveBodyTablePartitionIndex();
		try {
			List<Long> toBeRemoved = getJdbcTemplate().query(selectDanglingBodiesSQLs.get(partitionIndex), resultSet -> {
				List<Long> result = new ArrayList<>();
				while (resultSet.next()) {
					long bodyId = resultSet.getLong("bodyId");
					if (!resultSet.wasNull()) {
						result.add(bodyId);
					}
				}
				resultSet.close();
				return result;
			});
			if (toBeRemoved != null && !toBeRemoved.isEmpty()) {
				logger.debug("found " + toBeRemoved.size() + " dangling bodies to be removed from partition " + partitionIndex);
				Object[] params = new Object[removeDanglingBodiesBulkSize];
				Integer[] types = Collections.nCopies(removeDanglingBodiesBulkSize, Types.BIGINT).toArray(new Integer[0]);
				while (toBeRemoved.size() > 0) {
					List<Long> bulk = toBeRemoved.subList(0, Math.min(removeDanglingBodiesBulkSize, toBeRemoved.size()));
					Object[] bulkAsArray = bulk.toArray(new Long[0]);
					System.arraycopy(bulkAsArray, 0, params, 0, bulkAsArray.length);
					int removed = getJdbcTemplate().update(removeDanglingBodiesSQLs.get(partitionIndex), params, types);
					logger.debug("removed " + removed + " dangling bodies from partition " + partitionIndex);
					toBeRemoved.removeAll(bulk);
				}
			}
		} catch (DataAccessException dae) {
			logger.error("failed during cleaning dangling task bodies", dae);
		}
	}

	@Override
	public void handleStaledTasks() {
		getTransactionTemplate().execute(transactionStatus -> {
			String releaseLock = null;
			JdbcTemplate jdbcTemplate = getJdbcTemplate();
			try {
				String[] sqls = getSelectReRunnableStaledTasksSQL();
				String selectStaledSQL;
				if (sqls.length > 1) {
					jdbcTemplate.execute(sqls[0]);
					selectStaledSQL = sqls[1];
					if (sqls.length > 2) {
						releaseLock = sqls[2];
					}
				} else {
					selectStaledSQL = sqls[0];
				}
				List<ClusterTaskImpl> gcCandidates = jdbcTemplate.query(selectStaledSQL, this::gcCandidatesReader);
				if (gcCandidates != null && !gcCandidates.isEmpty()) {
					logger.info("found " + gcCandidates.size() + " re-runnable tasks as staled, processing...");

					//  collect tasks valid for re-enqueue
					Collection<ClusterTaskImpl> tasksToReschedule = gcCandidates.stream()
							.collect(Collectors.toMap(task -> task.processorType, Function.identity(), (t1, t2) -> t2))
							.values();

					//  reschedule tasks of SCHEDULED type
					int rescheduleResult = reinsertScheduledTasks(tasksToReschedule);
					logger.info("from " + tasksToReschedule.size() + " candidates for reschedule, " + rescheduleResult + " were actually rescheduled");
				}

				//  delete garbage tasks data
				int removed = jdbcTemplate.update(removeStaledTasksSQL);
				if (removed > 0) {
					logger.info("found and removed " + removed + " staled task/s");
				}
			} catch (Exception e) {
				transactionStatus.setRollbackOnly();
				throw new CtsGeneralFailure("failed to cleanup cluster tasks", e);
			} finally {
				if (releaseLock != null) {
					jdbcTemplate.execute(releaseLock);
				}
			}

			return null;
		});
	}

	@Override
	public int reinsertScheduledTasks(Collection<ClusterTaskImpl> candidatesToReschedule) {
		int result = 0;
		Map<String, Integer> pendingCount = getJdbcTemplate().query(countScheduledPendingTasksSQL, this::scheduledPendingReader);
		List<ClusterTaskImpl> tasksToReschedule = new ArrayList<>();
		candidatesToReschedule.forEach(task -> {
			if (pendingCount != null && (!pendingCount.containsKey(task.processorType) || pendingCount.get(task.processorType) == 0)) {
				task.uniquenessKey = task.processorType;
				task.concurrencyKey = task.processorType;
				tasksToReschedule.add(task);
			}
		});
		if (!tasksToReschedule.isEmpty()) {
			ClusterTaskPersistenceResult[] results = storeTasks(tasksToReschedule.toArray(new ClusterTaskImpl[0]));
			for (ClusterTaskPersistenceResult r : results) {
				if (r.getStatus() == ClusterTaskInsertStatus.SUCCESS) {
					result++;
				}
			}
		}
		return result;
	}

	@Override
	public Map<String, Integer> countTasks(ClusterTaskStatus status) {
		return getJdbcTemplate().query(countTasksByStatusSQL, new Object[]{status.value}, new int[]{Types.BIGINT}, resultSet -> {
			Map<String, Integer> result = new HashMap<>();
			while (resultSet.next()) {
				try {
					int counter = resultSet.getInt("counter");
					if (!resultSet.wasNull()) {
						result.put(resultSet.getString(PROCESSOR_TYPE), counter);
					} else {
						logger.error("received NULL value for count of pending tasks");
					}
				} catch (SQLException sqle) {
					logger.error("failed to process counted tasks result", sqle);
				}
			}
			resultSet.close();
			return result;
		});
	}

	@Override
	public int countTasksByApplicationKey(String applicationKey, ClusterTaskStatus status) {
		String sql = "SELECT COUNT(*) FROM " + META_TABLE_NAME +
				"   WHERE " + APPLICATION_KEY + (applicationKey == null ? " IS NULL" : " = ?") +
				(status == null ? "" : ("    AND " + STATUS + " = ?"));
		Integer result;
		if (applicationKey == null && status == null) {
			result = getJdbcTemplate().queryForObject(sql, Integer.class);
		} else {
			List<Object> values = new ArrayList<>();
			List<Integer> types = new ArrayList<>();
			if (applicationKey != null) {
				values.add(applicationKey);
				types.add(Types.VARCHAR);
			}
			if (status != null) {
				values.add(status.value);
				types.add(Types.BIGINT);
			}
			result = getJdbcTemplate().queryForObject(sql, values.toArray(new Object[0]), types.stream().mapToInt(Integer::intValue).toArray(), Integer.class);
		}
		return result != null ? result : 0;
	}

	@Override
	public Map<String, Integer> countBodies() {
		Map<String, Integer> result = new LinkedHashMap<>();
		for (Map.Entry<Long, String> sql : countTaskBodiesByPartitionSQLs.entrySet()) {
			getJdbcTemplate().query(sql.getValue(), resultSet -> {
				if (resultSet.next()) {
					try {
						int count = resultSet.getInt("counter");
						if (!resultSet.wasNull()) {
							result.put(String.valueOf(sql.getKey()), count);
						} else {
							logger.error("received NULL value for count of bodies, partition " + sql.getKey());
						}
					} catch (SQLException sqle) {
						logger.error("failed to process counted bodies result, partition " + sql.getKey(), sqle);
					}
				}
				resultSet.close();
				return null;
			});
		}
		return result;
	}

	@Override
	public void maintainStorage(boolean force) {
		ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
		int hour = now.getHour();
		int shiftDuration = 24 / PARTITIONS_NUMBER;
		//  run the truncation GC only:
		//      when left 1 hour before a roll over
		//      AND 45 minutes and above (so only 15 minutes till next roll over)
		//      AND truncate was not yet performed OR passed at least 1 hour since last truncate
		if (force ||
				(hour % shiftDuration == shiftDuration - 1 && now.getMinute() > 45 && (lastTruncateTime == null || Duration.between(lastTruncateTime, now).toHours() > 1))) {
			int currentPartition = hour / shiftDuration;
			int prevPartition = currentPartition == 0 ? PARTITIONS_NUMBER - 1 : currentPartition - 1;
			int nextPartition = currentPartition == PARTITIONS_NUMBER - 1 ? 0 : currentPartition + 1;
			logger.info((force ? "forced" : "timely") +
					" storage maintenance: current partition - " + currentPartition + ", next partition - " + nextPartition);
			for (long partition = 0; partition < PARTITIONS_NUMBER; partition++) {
				if (partition != currentPartition && partition != prevPartition && partition != nextPartition) {
					tryTruncateBodyTable(partition);
				}
			}
			lastTruncateTime = now;
		}
	}

	@Deprecated
	@Override
	public int countTasks(String processorType, Set<ClusterTaskStatus> statuses) {
		String countTasksSQL = buildCountTasksSQL(processorType, statuses);
		Integer result = getJdbcTemplate().queryForObject(countTasksSQL, Integer.class);
		return result != null ? result : 0;
	}

	JdbcTemplate getJdbcTemplate() {
		if (jdbcTemplate == null) {
			try {
				DataSource dataSource = serviceConfigurer.getDataSource();
				if (dataSource == null) {
					throw new IllegalStateException("hosting application's configurer failed to provide valid DataSource");
				} else {
					jdbcTemplate = new JdbcTemplate(dataSource);
				}
			} catch (Exception e) {
				throw new IllegalStateException("failed to create JdbcTemplate", e);
			}
		}
		return jdbcTemplate;
	}

	TransactionTemplate getTransactionTemplate() {
		if (transactionTemplate == null) {
			try {
				DataSource dataSource = serviceConfigurer.getDataSource();
				if (dataSource == null) {
					throw new IllegalStateException("hosting application's configurer failed to provide valid DataSource");
				} else {
					transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
				}
			} catch (Exception e) {
				throw new IllegalStateException("failed to create TransactionTemplate", e);
			}
		}
		return transactionTemplate;
	}

	List<ClusterTaskImpl> tasksMetadataReader(ResultSet resultSet) throws SQLException {
		List<ClusterTaskImpl> result = new LinkedList<>();
		ClusterTaskImpl tmpTask;
		Long tmpLong;
		while (resultSet.next()) {
			try {
				tmpTask = new ClusterTaskImpl();
				tmpTask.id = resultSet.getLong(META_ID);
				tmpTask.taskType = ClusterTaskType.byValue(resultSet.getLong(TASK_TYPE));
				tmpTask.processorType = resultSet.getString(PROCESSOR_TYPE);
				tmpTask.uniquenessKey = resultSet.getString(UNIQUENESS_KEY);
				tmpTask.concurrencyKey = resultSet.getString(CONCURRENCY_KEY);
				tmpTask.applicationKey = resultSet.getString(APPLICATION_KEY);
				tmpLong = resultSet.getLong(ORDERING_FACTOR);
				if (!resultSet.wasNull()) {
					tmpTask.orderingFactor = tmpLong;
				}
				tmpTask.delayByMillis = resultSet.getLong(DELAY_BY_MILLIS);
				tmpLong = resultSet.getLong(BODY_PARTITION);
				if (!resultSet.wasNull()) {
					tmpTask.partitionIndex = tmpLong;
				}

				result.add(tmpTask);
			} catch (Exception e) {
				logger.error("failed to read cluster task " + result.size(), e);
			}
		}
		resultSet.close();
		return result;
	}

	String rowToTaskBodyReader(ResultSet resultSet) throws SQLException {
		String result = null;
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
		resultSet.close();
		return result;
	}

	long resolveBodyTablePartitionIndex() {
		int hour = ZonedDateTime.now(ZoneOffset.UTC).getHour();
		return hour / (24 / PARTITIONS_NUMBER);
	}

	private Map<String, Integer> scheduledPendingReader(ResultSet resultSet) throws SQLException {
		Map<String, Integer> result = new LinkedHashMap<>();
		while (resultSet.next()) {
			try {
				result.put(resultSet.getString(PROCESSOR_TYPE), resultSet.getInt("total"));
			} catch (SQLException sqle) {
				logger.error("failed to read cluster task body", sqle);
			}
		}
		resultSet.close();
		return result;
	}

	private List<ClusterTaskImpl> gcCandidatesReader(ResultSet resultSet) throws SQLException {
		List<ClusterTaskImpl> result = new LinkedList<>();
		while (resultSet.next()) {
			try {
				ClusterTaskImpl task = new ClusterTaskImpl();
				task.id = resultSet.getLong(META_ID);
				task.taskType = ClusterTaskType.byValue(resultSet.getLong(TASK_TYPE));
				Long tmpLong = resultSet.getLong(BODY_PARTITION);
				if (!resultSet.wasNull()) {
					task.partitionIndex = tmpLong;
				}
				task.processorType = resultSet.getString(PROCESSOR_TYPE);
				task.delayByMillis = resultSet.getLong(DELAY_BY_MILLIS);
				result.add(task);
			} catch (SQLException sqle) {
				logger.error("failed to read cluster task body", sqle);
			}
		}
		resultSet.close();
		return result;
	}

	private void tryTruncateBodyTable(long partitionIndex) {
		logger.info("starting truncation of partition " + partitionIndex + "...");
		try {
			JdbcTemplate jdbcTemplate = getJdbcTemplate();
			String findAnyRowsInBodySQL = buildSelectVerifyBodyTableSQL(partitionIndex);
			String truncateBodyTableSQL = buildTruncateBodyTableSQL(partitionIndex);

			BodyTablePreTruncateVerificationResult verificationResult = jdbcTemplate.query(findAnyRowsInBodySQL, this::rowsToIDsInPartitionReader);
			if (verificationResult == null || verificationResult.getEntries().isEmpty()) {
				logger.info("... partition " + partitionIndex + " found empty, proceeding with truncate ...");
				jdbcTemplate.execute(truncateBodyTableSQL);
				logger.info("... partition " + partitionIndex + " truncate done");
			} else {
				int nonZombieBodies = 0;
				logger.warn("availability check during truncate partition " + partitionIndex + " found it's not empty (total of " + verificationResult.getEntries().size() + " entries)");
				logger.warn("bodies still having meta (non-zombie):");
				for (BodyTablePreTruncateVerificationResult.Entry entry : verificationResult.getEntries()) {
					if (entry.metaId != null) {
						logger.warn("\t" + entry);
						nonZombieBodies++;
					}
				}
				logger.warn("total of non-zombie bodies: " + nonZombieBodies);
				if (nonZombieBodies == 0) {
					logger.info("... partition " + partitionIndex + " found non-empty, but all of it's entries considered 'zombies', proceeding with truncate ...");
					jdbcTemplate.execute(truncateBodyTableSQL);
					logger.info("... partition " + partitionIndex + " truncate done");
				} else {
					logger.warn("... partition " + partitionIndex + " found non-empty, and " + nonZombieBodies + " of it's entries are not 'zombies', will not truncate");
					int zombieBodies = verificationResult.getEntries().size() - nonZombieBodies;
					if (zombieBodies > 0) {
						logger.info("removing " + zombieBodies + " abandoned bodies...");
						String sql = "DELETE" +
								"   FROM " + BODY_TABLE_NAME + partitionIndex + " b" +
								"   WHERE NOT EXISTS (" +
								"       SELECT NULL" +
								"       FROM " + META_TABLE_NAME + " m" +
								"       WHERE   m." + META_ID + " = b." + BODY_ID +
								"   )";
						int updated = jdbcTemplate.update(sql);
						logger.info(updated + " abandoned bodies were successfully removed");
					}
				}
			}
		} catch (Exception e) {
			logger.error("failed to truncate partition " + partitionIndex, e);
		}
	}

	private String buildCountTasksSQL(String processorType, Set<ClusterTaskStatus> statuses) {
		List<String> queryClauses = new LinkedList<>();
		if (processorType != null) {
			queryClauses.add(PROCESSOR_TYPE + " = '" + processorType + "'");
		}
		if (statuses != null && !statuses.isEmpty()) {
			queryClauses.add(STATUS + " IN (" + statuses.stream().map(status -> String.valueOf(status.value)).collect(Collectors.joining(",")) + ")");
		}

		return buildCountSQLByQueries(queryClauses);
	}

	private static String buildCountSQLByQueries(List<String> queryClauses) {
		return "SELECT COUNT(*) FROM " + META_TABLE_NAME +
				(queryClauses.isEmpty() ? "" : " WHERE " + String.join(" AND ", queryClauses));
	}

	private String buildSelectVerifyBodyTableSQL(long partitionIndex) {
		return lookupOrphansByPartitionSQLs.get(partitionIndex);
	}

	private String buildTruncateBodyTableSQL(long partitionIndex) {
		return truncateByPartitionSQLs.get(partitionIndex);
	}

	private BodyTablePreTruncateVerificationResult rowsToIDsInPartitionReader(ResultSet resultSet) {
		BodyTablePreTruncateVerificationResult result = new BodyTablePreTruncateVerificationResult();
		try {
			while (resultSet.next()) {
				try {
					Long metaId;

					metaId = resultSet.getLong(META_ID);
					if (resultSet.wasNull()) {
						metaId = null;
					}

					result.addEntry(metaId);
				} catch (SQLException sqle) {
					logger.error("failed to read cluster task (body ID)", sqle);
				}
			}
		} catch (SQLException sqle) {
			logger.error("failed to perform empty body table data collection", sqle);
		}
		return result;
	}

	private static class BodyTablePreTruncateVerificationResult {
		private final List<Entry> entries = new LinkedList<>();

		void addEntry(Long metaId) {
			entries.add(new Entry(metaId));
		}

		List<Entry> getEntries() {
			return entries;
		}

		private static class Entry {
			final Long metaId;

			private Entry(Long metaId) {
				this.metaId = metaId;
			}

			@Override
			public String toString() {
				return "metaId: " + metaId;
			}
		}
	}
}
