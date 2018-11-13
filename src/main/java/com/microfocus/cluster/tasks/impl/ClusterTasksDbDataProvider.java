/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.impl;

import com.microfocus.cluster.tasks.api.enums.ClusterTaskStatus;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskType;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.cluster.tasks.api.ClusterTasksService;
import com.microfocus.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.microfocus.cluster.tasks.api.errors.CtsSqlFailure;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import static java.sql.Types.BIGINT;

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

	private final Map<Long, String> lookupOrphansByPartitionSQLs = new LinkedHashMap<>();
	private final Map<Long, String> deleteTaskBodyByPartitionSQLs = new LinkedHashMap<>();
	private final Map<Long, String> truncateByPartitionSQLs = new LinkedHashMap<>();

	private final String countTasksByStatusSQL;
	private final Map<Long, String> countTaskBodiesByPartitionSQLs = new LinkedHashMap<>();

	private ZonedDateTime lastTruncateTime;
	private JdbcTemplate jdbcTemplate;
	private TransactionTemplate transactionTemplate;

	ClusterTasksDbDataProvider(ClusterTasksService clusterTasksService, ClusterTasksServiceConfigurerSPI serviceConfigurer) {
		if (clusterTasksService == null) {
			throw new IllegalArgumentException("cluster tasks service MUST NOT be null");
		}
		if (serviceConfigurer == null) {
			throw new IllegalArgumentException("service configurer MUST NOT be null");
		}
		this.clusterTasksService = clusterTasksService;
		this.serviceConfigurer = serviceConfigurer;

		//  prepare SQL statements
		removeFinishedTaskSQL = "DELETE FROM " + META_TABLE_NAME + " WHERE " + META_ID + " = ?";
		removeFinishedTasksByQuerySQL = "DELETE FROM " + META_TABLE_NAME + " WHERE " + STATUS + " = " + ClusterTaskStatus.FINISHED.value;

		for (long partition = 0; partition < PARTITIONS_NUMBER; partition++) {
			lookupOrphansByPartitionSQLs.put(partition, "SELECT " + String.join(",", BODY_ID, BODY, META_ID) + " FROM " + BODY_TABLE_NAME + partition +
					" LEFT OUTER JOIN " + META_TABLE_NAME + " ON " + META_ID + " = " + BODY_ID);
			deleteTaskBodyByPartitionSQLs.put(partition, "DELETE FROM " + BODY_TABLE_NAME + partition +
					" WHERE " + BODY_ID + " = ?");

			selectDanglingBodiesSQLs.put(partition, "SELECT " + BODY_ID + " AS bodyId FROM " + BODY_TABLE_NAME + partition +
					" WHERE NOT EXISTS (SELECT 1 FROM " + META_TABLE_NAME + " WHERE " + META_ID + " = " + BODY_ID + ")");
			removeDanglingBodiesSQLs.put(partition, "DELETE FROM " + BODY_TABLE_NAME + partition + " WHERE " + BODY_ID + " IN (" + String.join(",", Collections.nCopies(removeDanglingBodiesBulkSize, "?")) + ")");

			truncateByPartitionSQLs.put(partition, "TRUNCATE TABLE " + BODY_TABLE_NAME + partition);
			countTaskBodiesByPartitionSQLs.put(partition, "SELECT COUNT(*) AS counter FROM " + BODY_TABLE_NAME + partition);
		}

		countTasksByStatusSQL = "SELECT COUNT(*) AS counter," + PROCESSOR_TYPE + " FROM " + META_TABLE_NAME + " WHERE " + STATUS + " = ? GROUP BY " + PROCESSOR_TYPE;
	}

	@Override
	public ClusterTasksDataProviderType getType() {
		return ClusterTasksDataProviderType.DB;
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
			if (!toBeRemoved.isEmpty()) {
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

	@Deprecated
	@Override
	public int countTasks(String processorType, Set<ClusterTaskStatus> statuses) {
		String countTasksSQL = buildCountTasksSQL(processorType, statuses);
		return getJdbcTemplate().queryForObject(countTasksSQL, Integer.class);
	}

	@Deprecated
	@Override
	public int countTasks(String processorType, String concurrencyKey, Set<ClusterTaskStatus> statuses) {
		String countTasksSQL = buildCountTasksSQL(processorType, concurrencyKey, statuses);
		return getJdbcTemplate().queryForObject(countTasksSQL, Integer.class);
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

	List<TaskInternal> tasksMetadataReader(ResultSet resultSet) throws SQLException {
		List<TaskInternal> result = new LinkedList<>();
		TaskInternal tmpTask;
		Long tmpLong;
		String tmpString;
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
				tmpLong = resultSet.getLong(ClusterTasksDbDataProvider.BODY_PARTITION);
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
				Clob clobBody = resultSet.getClob(ClusterTasksDbDataProvider.BODY);
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

	Map<String, Integer> scheduledPendingReader(ResultSet resultSet) throws SQLException {
		Map<String, Integer> result = new LinkedHashMap<>();
		while (resultSet.next()) {
			try {
				result.put(resultSet.getString(ClusterTasksDbDataProvider.PROCESSOR_TYPE), resultSet.getInt("total"));
			} catch (SQLException sqle) {
				logger.error("failed to read cluster task body", sqle);
			}
		}
		resultSet.close();
		return result;
	}

	List<TaskInternal> gcCandidatesReader(ResultSet resultSet) throws SQLException {
		List<TaskInternal> result = new LinkedList<>();
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
				result.add(task);
			} catch (SQLException sqle) {
				logger.error("failed to read cluster task body", sqle);
			}
		}
		resultSet.close();
		return result;
	}

	long resolveBodyTablePartitionIndex() {
		int hour = ZonedDateTime.now(ZoneOffset.UTC).getHour();
		return hour / (24 / PARTITIONS_NUMBER);
	}

	//  [YG] TODO: fix this, since it is NOT doing its job now
	void deleteGarbageTasksData(JdbcTemplate jdbcTemplate, Map<Long, Long> taskIDsBodyPartitionsMap) {
		try {
			//  delete metas
			List<Object[]> mParams = taskIDsBodyPartitionsMap.keySet().stream()
					.sorted()
					.map(id -> new Object[]{id})
					.collect(Collectors.toList());
			int[] deletedMetas = jdbcTemplate.batchUpdate(removeFinishedTaskSQL, mParams, new int[]{BIGINT});
			logger.debug("deleted " + deletedMetas.length + " task/s (metadata)");

			//  delete bodies
			Map<Long, Set<Long>> partitionsToIdsMap = new LinkedHashMap<>();
			taskIDsBodyPartitionsMap.forEach((taskId, partitionIndex) -> {
				if (partitionIndex != null) {
					partitionsToIdsMap
							.computeIfAbsent(partitionIndex, pId -> new LinkedHashSet<>())
							.add(taskId);
				}
			});
			partitionsToIdsMap.forEach((partitionId, taskIDsInPartition) -> {
				String deleteTimedoutBodySQL = buildDeleteTaskBodySQL(partitionId);
				List<Object[]> bParams = taskIDsInPartition.stream()
						.map(id -> new Object[]{id})
						.collect(Collectors.toList());
				int[] deletedBodies = jdbcTemplate.batchUpdate(deleteTimedoutBodySQL, bParams, new int[]{BIGINT});
				logger.debug("deleted " + deletedBodies.length + " task/s (content)");
			});

			//  truncate currently non-active body tables (that are safe to truncate)
			checkAndTruncateBodyTables();
		} catch (Exception e) {
			logger.error("failed to delete Garbage tasks data", e);
		}
	}

	void checkAndTruncateBodyTables() {
		ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
		int hour = now.getHour();
		int shiftDuration = 24 / PARTITIONS_NUMBER;
		//  run the truncation GC only:
		//      when left 1 hour before a roll over
		//      AND 45 minutes and above (so only 15 minutes till next roll over)
		//      AND truncate was not yet performed OR passed at least 1 hour since last truncate
		if (hour % shiftDuration == shiftDuration - 1 &&
				now.getMinute() > 45 &&
				(lastTruncateTime == null || Duration.between(lastTruncateTime, now).toHours() > 1)) {
			int currentPartition = hour / shiftDuration;
			int prevPartition = currentPartition == 0 ? PARTITIONS_NUMBER - 1 : currentPartition - 1;
			int nextPartition = currentPartition == PARTITIONS_NUMBER - 1 ? 0 : currentPartition + 1;
			logger.info("timely task body tables partitions truncate maintenance: current partition - " + currentPartition + ", next partition - " + nextPartition);
			for (long partition = 0; partition < PARTITIONS_NUMBER; partition++) {
				if (partition != currentPartition && partition != prevPartition && partition != nextPartition) {
					tryTruncateBodyTable(partition);
				}
			}
			lastTruncateTime = now;
		}
	}

	private void tryTruncateBodyTable(long partitionIndex) {
		logger.info("starting truncation of partition " + partitionIndex + "...");
		try {
			JdbcTemplate jdbcTemplate = getJdbcTemplate();
			String findAnyRowsInBodySQL = buildSelectVerifyBodyTableSQL(partitionIndex);
			String truncateBodyTableSQL = buildTruncateBodyTableSQL(partitionIndex);

			BodyTablePreTruncateVerificationResult verificationResult = jdbcTemplate.query(findAnyRowsInBodySQL, this::rowsToIDsInPartitionReader);
			if (verificationResult.getEntries().isEmpty()) {
				logger.info("... partition " + partitionIndex + " found empty, proceeding with truncate ...");
				jdbcTemplate.execute(truncateBodyTableSQL);
				logger.info("... partition " + partitionIndex + " truncate done");
			} else {
				int nonZombieBodies = 0;
				logger.warn("availability check during truncate partition " + partitionIndex + " found it's not empty (" + verificationResult.getEntries().size() + " entries)");
				for (BodyTablePreTruncateVerificationResult.Entry entry : verificationResult.getEntries()) {
					logger.warn("--- " + entry);
					if (entry.metaId != null) {
						nonZombieBodies++;
					}
				}
				logger.warn("--- total bodies still having meta (non-zombie): " + nonZombieBodies);
				if (nonZombieBodies == 0) {
					logger.info("... partition " + partitionIndex + " found non-empty, but all of it's entries considered 'zombies', proceeding with truncate ...");
					jdbcTemplate.execute(truncateBodyTableSQL);
					logger.info("... partition " + partitionIndex + " truncate done");
				} else {
					logger.warn("... partition " + partitionIndex + " found non-empty, and " + nonZombieBodies + " of it's entries are not 'zombies', will not truncate");
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
			queryClauses.add(STATUS + " IN (" + String.join(",", statuses.stream().map(status -> String.valueOf(status.value)).collect(Collectors.toList())) + ")");
		}

		return buildCountSQLByQueries(queryClauses);
	}

	private String buildCountTasksSQL(String processorType, String concurrencyKey, Set<ClusterTaskStatus> statuses) {
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

	private static String buildCountSQLByQueries(List<String> queryClauses) {
		return "SELECT COUNT(*) FROM " + META_TABLE_NAME +
				(queryClauses.isEmpty() ? "" : " WHERE " + String.join(" AND ", queryClauses));
	}

	private String buildDeleteTaskBodySQL(long partitionIndex) {
		return deleteTaskBodyByPartitionSQLs.get(partitionIndex);
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

					result.addEntry(metaId, bodyId, body);
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

		void addEntry(Long metaId, Long bodyId, String body) {
			entries.add(new Entry(metaId, bodyId, body));
		}

		List<Entry> getEntries() {
			return entries;
		}

		private static class Entry {
			final Long metaId;
			final Long bodyId;
			final String body;

			private Entry(Long metaId, Long bodyId, String body) {
				this.metaId = metaId;
				this.bodyId = bodyId;
				this.body = body;
			}
		}
	}
}
