/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.impl;

import com.microfocus.cluster.tasks.api.ClusterTasksService;
import com.microfocus.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.microfocus.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.microfocus.cluster.tasks.api.enums.CTPPersistStatus;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskStatus;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskType;
import com.microfocus.cluster.tasks.api.errors.CtsGeneralFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.sql.Types.BIGINT;
import static java.sql.Types.VARCHAR;

/**
 * Created by gullery on 12/04/2018.
 *
 * MsSql Server oriented data provider
 */

final class MsSqlDbDataProvider extends ClusterTasksDbDataProvider {
	private static final Logger logger = LoggerFactory.getLogger(MsSqlDbDataProvider.class);

	private final String areTablesReadySQL;
	private final String areIndicesReadySQL;
	private final String areSequencesReadySQL;

	private final String insertSelfLastSeenSQL;
	private final String updateSelfLastSeenSQL;
	private final String removeLongTimeNoSeeSQL;

	private final String insertTaskWithoutBodySQL;
	private final Map<Long, String> insertTaskWithBodySQLs = new HashMap<>();
	private final Map<Integer, String> selectForUpdateTasksSQLs = new HashMap<>();
	private final Map<Long, String> selectTaskBodyByPartitionSQLs = new HashMap<>();

	private final String updateTasksStartedSQL;
	private final String updateTaskFinishedSQL;

	private final String countScheduledPendingTasksSQL;

	private final String selectStaledRerunnableTasksSQL;
	private final String removeStaledTasksSQL;

	MsSqlDbDataProvider(ClusterTasksService clusterTasksService, ClusterTasksServiceConfigurerSPI serviceConfigurer) {
		super(clusterTasksService, serviceConfigurer);

		//  prepare SQL statements
		areTablesReadySQL = "SELECT COUNT(*) AS cts_tables_count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME IN(" +
				String.join(",", getCTSTableNames().stream().map(tn -> "'" + tn + "'").collect(Collectors.toSet())) + ")";
		areIndicesReadySQL = "SELECT COUNT(*) AS cts_indices_count FROM sys.indexes WHERE name IN(" +
				String.join(",", getCTSIndexNames().stream().map(in -> "'" + in + "'").collect(Collectors.toSet())) + ")";
		areSequencesReadySQL = "SELECT COUNT(*) AS cts_sequences_count FROM sys.sequences WHERE name IN(" +
				String.join(",", getCTSSequenceNames().stream().map(sn -> "'" + sn + "'").collect(Collectors.toSet())) + ")";

		insertSelfLastSeenSQL = "INSERT INTO " + ACTIVE_NODES_TABLE_NAME + " (" + ACTIVE_NODE_ID + ", " + ACTIVE_NODE_SINCE + ", " + ACTIVE_NODE_LAST_SEEN + ") VALUES (?, GETDATE(), GETDATE())";
		updateSelfLastSeenSQL = "UPDATE " + ACTIVE_NODES_TABLE_NAME + " SET " + ACTIVE_NODE_LAST_SEEN + " = GETDATE() WHERE " + ACTIVE_NODE_ID + " = ?";
		removeLongTimeNoSeeSQL = "DELETE FROM " + ACTIVE_NODES_TABLE_NAME + " WHERE " + ACTIVE_NODE_LAST_SEEN + " < DATEADD(MILLISECOND, -?, GETDATE())";

		String insertFields = String.join(",", META_ID, TASK_TYPE, PROCESSOR_TYPE, UNIQUENESS_KEY, CONCURRENCY_KEY, DELAY_BY_MILLIS, BODY_PARTITION, ORDERING_FACTOR, CREATED, STATUS);
		insertTaskWithoutBodySQL = "INSERT INTO " + META_TABLE_NAME + " (" + insertFields + ")" +
				" VALUES (NEXT VALUE FOR " + CLUSTER_TASK_ID_SEQUENCE + ", ?, ?, ?, ?, ?, ?, COALESCE(?, CAST(FORMAT(CURRENT_TIMESTAMP,'yyyyMMddHHmmssfff') AS BIGINT) + ?), GETDATE(), " + ClusterTaskStatus.PENDING.value + ")";

		String selectFields = String.join(",", META_ID, TASK_TYPE, PROCESSOR_TYPE, UNIQUENESS_KEY, CONCURRENCY_KEY, ORDERING_FACTOR, DELAY_BY_MILLIS, BODY_PARTITION, STATUS);
		for (int maxProcessorTypes : new Integer[]{20, 50, 100, 500}) {
			String processorTypesInParameter = String.join(",", Collections.nCopies(maxProcessorTypes, "?"));
			selectForUpdateTasksSQLs.put(maxProcessorTypes, "SELECT * FROM" +
					"       (SELECT " + selectFields + "," +
					"               ROW_NUMBER() OVER (PARTITION BY COALESCE(" + CONCURRENCY_KEY + ",CAST(NEWID() AS VARCHAR(36))) ORDER BY " + ORDERING_FACTOR + "," + CREATED + "," + META_ID + " ASC) AS row_index," +
					"               COUNT(CASE WHEN " + STATUS + " = " + ClusterTaskStatus.RUNNING.value + " THEN 1 ELSE NULL END) OVER (PARTITION BY COALESCE(" + CONCURRENCY_KEY + ",CAST(NEWID() AS VARCHAR(36)))) AS running_count" +
					"       FROM " + META_TABLE_NAME + " WITH (UPDLOCK,TABLOCK)" +
					"       WHERE " + PROCESSOR_TYPE + " IN(" + processorTypesInParameter + ")" +
					"           AND " + STATUS + " < " + ClusterTaskStatus.FINISHED.value +
					"           AND " + CREATED + " < DATEADD(MILLISECOND, -" + DELAY_BY_MILLIS + ", GETDATE())) meta" +
					"   WHERE meta.row_index <= 1 AND meta.running_count = 0");
		}
		for (long partition = 0; partition < PARTITIONS_NUMBER; partition++) {
			selectTaskBodyByPartitionSQLs.put(partition, "SELECT " + BODY + " FROM " + BODY_TABLE_NAME + partition +
					" WHERE " + BODY_ID + " = ?");
			insertTaskWithBodySQLs.put(partition, "DECLARE @taskId BIGINT = NEXT VALUE FOR " + CLUSTER_TASK_ID_SEQUENCE + ";" +
					" INSERT INTO " + BODY_TABLE_NAME + partition + " (" + BODY_ID + "," + BODY + ") VALUES (@taskId, ?);" +
					" INSERT INTO " + META_TABLE_NAME + " (" + insertFields + ")" +
					" VALUES (@taskId, ?, ?, ?, ?, ?, ?, COALESCE(?, CAST(FORMAT(CURRENT_TIMESTAMP,'yyyyMMddHHmmssfff') AS BIGINT) + ?), GETDATE(), " + ClusterTaskStatus.PENDING.value + ");"
			);
		}

		updateTasksStartedSQL = "UPDATE " + META_TABLE_NAME + " SET " + STATUS + " = " + ClusterTaskStatus.RUNNING.value + ", " + STARTED + " = GETDATE(), " + RUNTIME_INSTANCE + " = ?" +
				" WHERE " + META_ID + " = ?";
		updateTaskFinishedSQL = "UPDATE " + META_TABLE_NAME + " SET " + String.join(",", STATUS + " = " + ClusterTaskStatus.FINISHED.value, UNIQUENESS_KEY + " = CAST(NEWID() AS VARCHAR(36))") +
				" WHERE " + META_ID + " = ?";

		countScheduledPendingTasksSQL = "SELECT " + PROCESSOR_TYPE + ",COUNT(*) AS total FROM " + META_TABLE_NAME +
				" WHERE " + TASK_TYPE + " = " + ClusterTaskType.SCHEDULED.value + " AND " + STATUS + " = " + ClusterTaskStatus.PENDING.value + " GROUP BY " + PROCESSOR_TYPE;

		String selectedForGCFields = String.join(",", META_ID, BODY_PARTITION, TASK_TYPE, PROCESSOR_TYPE, STATUS, RUNTIME_INSTANCE);
		selectStaledRerunnableTasksSQL = "SELECT " + selectedForGCFields + " FROM " + META_TABLE_NAME + " WITH (UPDLOCK)" +
				" WHERE " + TASK_TYPE + " = " + ClusterTaskType.SCHEDULED.value +
				"   AND " + RUNTIME_INSTANCE + " IS NOT NULL" +
				"   AND NOT EXISTS (SELECT 1 FROM " + ACTIVE_NODES_TABLE_NAME + " WHERE " + ACTIVE_NODE_ID + " = " + RUNTIME_INSTANCE + ")";
		removeStaledTasksSQL = "DELETE FROM " + META_TABLE_NAME +
				" WHERE " + TASK_TYPE + " = " + ClusterTaskType.REGULAR.value +
				"   AND" + RUNTIME_INSTANCE + " IS NOT NULL" +
				"   AND NOT EXISTS (SELECT 1 FROM " + ACTIVE_NODES_TABLE_NAME + " WHERE " + ACTIVE_NODE_ID + " = " + RUNTIME_INSTANCE + ")";
	}

	@Override
	public boolean isReady() {
		if (isReady == null || !isReady) {
			String sql;
			String dataProviderName = this.getClass().getSimpleName();
			logger.info("going to verify readiness of " + dataProviderName);

			//  check tables existence
			Set<String> tableNames = getCTSTableNames();
			sql = areTablesReadySQL;
			int tablesCount = getJdbcTemplate().queryForObject(sql, (resultSet, index) -> resultSet.getInt("cts_tables_count"));
			if (tablesCount == tableNames.size()) {
				//  check indices existence
				Set<String> indexNames = getCTSIndexNames();
				sql = areIndicesReadySQL;
				int indicesCount = getJdbcTemplate().queryForObject(sql, (resultSet, index) -> resultSet.getInt("cts_indices_count"));
				if (indicesCount == indexNames.size()) {
					//  check sequences existence
					Set<String> sequenceNames = getCTSSequenceNames();
					sql = areSequencesReadySQL;
					int sequencesCount = getJdbcTemplate().queryForObject(sql, (resultSet, index) -> resultSet.getInt("cts_sequences_count"));
					if (sequencesCount == sequenceNames.size()) {
						logger.info(dataProviderName + " found to be READY");
						isReady = true;
					} else {
						logger.warn(dataProviderName + " found being NOT READY: expected number of sequences - " + sequenceNames.size() + ", found - " + sequencesCount);
						return false;
					}
				} else {
					logger.warn(dataProviderName + " found being NOT READY: expected number of indices - " + indexNames.size() + ", found - " + indicesCount);
					isReady = false;
				}
			} else {
				logger.warn(dataProviderName + " found being NOT READY: expected number of tables - " + tableNames.size() + ", found - " + tablesCount);
				isReady = false;
			}
		}

		return isReady;
	}

	@Override
	public ClusterTaskPersistenceResult[] storeTasks(TaskInternal... tasks) {
		List<ClusterTaskPersistenceResult> result = new ArrayList<>(tasks.length);

		for (TaskInternal task : tasks) {
			getTransactionTemplate().execute(transactionStatus -> {
				try {
					JdbcTemplate jdbcTemplate = getJdbcTemplate();

					//  prepare contents
					String insertTaskSql;
					Object[] paramValues;
					int[] paramTypes;
					boolean hasBody = task.body != null;
					if (hasBody) {
						task.partitionIndex = resolveBodyTablePartitionIndex();
						insertTaskSql = insertTaskWithBodySQLs.get(task.partitionIndex);
						paramValues = new Object[]{
								task.body,
								task.taskType.value,
								task.processorType,
								task.uniquenessKey,
								task.concurrencyKey,
								task.delayByMillis,
								task.partitionIndex,
								task.orderingFactor,
								task.delayByMillis
						};
						paramTypes = new int[]{
								Types.CLOB,                 //  task body -  will be used only if actually has body
								Types.BIGINT,               //  task type
								Types.VARCHAR,              //  processor type
								Types.VARCHAR,              //  uniqueness key
								Types.VARCHAR,              //  concurrency key
								Types.BIGINT,               //  delay by millis
								Types.BIGINT,               //  partition index
								Types.BIGINT,               //  ordering factor
								Types.BIGINT                //  delay by millis (second time for potential ordering calculation based on creation time when ordering is NULL)
						};
					} else {
						insertTaskSql = insertTaskWithoutBodySQL;
						paramValues = new Object[]{
								task.taskType.value,
								task.processorType,
								task.uniquenessKey,
								task.concurrencyKey,
								task.delayByMillis,
								task.partitionIndex,
								task.orderingFactor,
								task.delayByMillis
						};
						paramTypes = new int[]{
								Types.BIGINT,               //  task type
								Types.VARCHAR,              //  processor type
								Types.VARCHAR,              //  uniqueness key
								Types.VARCHAR,              //  concurrency key
								Types.BIGINT,               //  delay by millis
								Types.BIGINT,               //  partition index
								Types.BIGINT,               //  ordering factor
								Types.BIGINT                //  delay by millis (second time for potential ordering calculation based on creation time when ordering is NULL)
						};
					}

					//  insert task
					jdbcTemplate.update(insertTaskSql, paramValues, paramTypes);
					result.add(new ClusterTaskPersistenceResultImpl(CTPPersistStatus.SUCCESS));
					if (logger.isDebugEnabled()) {
						logger.debug("successfully created " + task);
					}
				} catch (DuplicateKeyException dke) {
					transactionStatus.setRollbackOnly();
					result.add(new ClusterTaskPersistenceResultImpl(CTPPersistStatus.UNIQUE_CONSTRAINT_FAILURE));
					logger.info(clusterTasksService.getInstanceID() + " rejected " + task + " due to uniqueness violation; specifically: " + dke.getMostSpecificCause().getMessage());
				} catch (Exception e) {
					transactionStatus.setRollbackOnly();
					result.add(new ClusterTaskPersistenceResultImpl(CTPPersistStatus.UNEXPECTED_FAILURE));
					logger.error(clusterTasksService.getInstanceID() + " failed to persist " + task, e);
				}
				return null;
			});
		}

		return result.toArray(new ClusterTaskPersistenceResult[0]);
	}

	@Override
	public void retrieveAndDispatchTasks(Map<String, ClusterTasksProcessorBase> availableProcessors) {
		Map<ClusterTasksProcessorBase, Collection<TaskInternal>> tasksToRun = new HashMap<>();

		//  within the same transaction do:
		//  - SELECT candidate tasks to be run
		//  - LET processors to pick up the tasks that will actually run
		//  - UPDATE those tasks as RUNNING
		getTransactionTemplate().execute(transactionStatus -> {
			try {
				JdbcTemplate jdbcTemplate = getJdbcTemplate();
				String[] availableProcessorTypes = availableProcessors.keySet().toArray(new String[0]);
				Integer paramsTotal = null;
				String sql = null;
				for (Map.Entry<Integer, String> testedParam : selectForUpdateTasksSQLs.entrySet()) {
					if ((paramsTotal = testedParam.getKey()) >= availableProcessors.size()) {
						sql = testedParam.getValue();
						break;
					}
				}
				if (paramsTotal == null || sql == null) {
					throw new IllegalStateException("failed to match 'selectForUpdateTasks' SQL for the amount of " + availableProcessors.size() + " processors");
				}

				//  prepare params
				Object[] params = new Object[paramsTotal];
				System.arraycopy(availableProcessorTypes, 0, params, 0, availableProcessorTypes.length);
				for (int i = availableProcessorTypes.length; i < paramsTotal; i++) params[i] = null;

				//  prepare param types
				int[] paramTypes = new int[paramsTotal];
				for (int i = 0; i < paramsTotal; i++) paramTypes[i] = Types.NVARCHAR;

				List<TaskInternal> tasks;
				tasks = jdbcTemplate.query(sql, params, paramTypes, this::tasksMetadataReader);
				if (!tasks.isEmpty()) {
					Map<String, List<TaskInternal>> tasksByProcessor = tasks.stream().collect(Collectors.groupingBy(ti -> ti.processorType));
					Set<Long> tasksToRunIDs = new HashSet<>();

					//  let processors decide which tasks will be processed from all available
					tasksByProcessor.forEach((processorType, processorTasks) -> {
						ClusterTasksProcessorBase processor = availableProcessors.get(processorType);
						Collection<TaskInternal> tmpTasks = processor.selectTasksToRun(processorTasks);
						tasksToRun.put(processor, tmpTasks);
						tasksToRunIDs.addAll(tmpTasks.stream().map(task -> task.id).collect(Collectors.toList()));
					});

					//  update selected tasks to RUNNING
					if (!tasksToRunIDs.isEmpty()) {
						String runtimeInstanceID = clusterTasksService.getInstanceID();
						List<Object[]> updateParams = tasksToRunIDs.stream()
								.sorted()
								.map(id -> new Object[]{runtimeInstanceID, id})
								.collect(Collectors.toList());
						int[] updateResults = jdbcTemplate.batchUpdate(updateTasksStartedSQL, updateParams, new int[]{VARCHAR, BIGINT});
						if (logger.isDebugEnabled()) {
							logger.debug("update tasks to RUNNING results: " + String.join(", ", Stream.of(updateResults).map(String::valueOf).collect(Collectors.toList())));
							logger.debug("from a total of " + tasks.size() + " available tasks " + tasksToRunIDs.size() + " has been started");
						}
					} else {
						logger.warn("from a total of " + tasks.size() + " available tasks none has been started");
					}
				}
			} catch (Throwable t) {
				transactionStatus.setRollbackOnly();
				tasksToRun.clear();
				throw new CtsGeneralFailure("failed to retrieve and execute tasks", t);
			}

			return null;
		});

		//  actually deliver tasks to processors
		tasksToRun.forEach((processor, tasks) -> processor.handleTasks(tasks, this));
	}

	@Override
	public String retrieveTaskBody(Long taskId, Long partitionIndex) {
		if (taskId == null) {
			throw new IllegalArgumentException("task ID MUST NOT be null");
		}
		if (partitionIndex == null) {
			throw new IllegalArgumentException("partition index MUST NOT be null");
		}

		try {
			JdbcTemplate jdbcTemplate = getJdbcTemplate();
			String sql = selectTaskBodyByPartitionSQLs.get(partitionIndex);
			return jdbcTemplate.query(
					sql,
					new Object[]{taskId},
					new int[]{BIGINT},
					this::rowToTaskBodyReader);
		} catch (Exception e) {
			throw new CtsGeneralFailure(clusterTasksService.getInstanceID() + " failed to retrieve task's body", e);
		}
	}

	@Override
	public void updateTaskToFinished(Long taskId) {
		try {
			getJdbcTemplate().update(updateTaskFinishedSQL, new Object[]{taskId}, new int[]{BIGINT});
		} catch (DataAccessException dae) {
			throw new CtsGeneralFailure(clusterTasksService.getInstanceID() + " failed to update task finished", dae);
		}
	}

	@Override
	public void handleStaledTasks() {
		getTransactionTemplate().execute(transactionStatus -> {
			try {
				JdbcTemplate jdbcTemplate = getJdbcTemplate();
				List<TaskInternal> gcCandidates = jdbcTemplate.query(selectStaledRerunnableTasksSQL, this::gcCandidatesReader);
				if (gcCandidates.size() > 0) {
					logger.info("found " + gcCandidates.size() + " re-runnable tasks as staled, processing...");

					//  collect tasks valid for re-enqueue
					Collection<TaskInternal> tasksToReschedule = gcCandidates.stream()
							.collect(Collectors.toMap(task -> task.processorType, Function.identity(), (t1, t2) -> t2))
							.values();

					//  reschedule tasks of SCHEDULED type
					reinsertScheduledTasks(tasksToReschedule);

					//  delete garbage tasks data
					int removed = jdbcTemplate.update(removeStaledTasksSQL);
					if (removed > 0) {
						logger.info("found and removed " + removed + " staled tasks");
					}
				}
			} catch (Exception e) {
				transactionStatus.setRollbackOnly();
				throw new CtsGeneralFailure("failed to cleanup cluster tasks", e);
			}

			return null;
		});

		checkAndTruncateBodyTables();
	}

	@Override
	public void reinsertScheduledTasks(Collection<TaskInternal> candidatesToReschedule) {
		Map<String, Integer> pendingCount = getJdbcTemplate().query(countScheduledPendingTasksSQL, this::scheduledPendingReader);
		List<TaskInternal> tasksToReschedule = new ArrayList<>();
		candidatesToReschedule.forEach(task -> {
			if (!pendingCount.containsKey(task.processorType) || pendingCount.get(task.processorType) == 0) {
				task.uniquenessKey = task.processorType;
				task.concurrencyKey = task.processorType;
				task.delayByMillis = 0L;
				tasksToReschedule.add(task);
			}
		});
		if (!tasksToReschedule.isEmpty()) {
			storeTasks(tasksToReschedule.toArray(new TaskInternal[0]));
		}
	}

	@Override
	public void updateSelfLastSeen(String nodeId) {
		int updated = getJdbcTemplate().update(updateSelfLastSeenSQL, new Object[]{nodeId}, new int[]{Types.VARCHAR});
		if (updated == 0) {
			logger.info("node " + nodeId + " activity was NOT UPDATED, performing initial registration...");
			int affected = getJdbcTemplate().update(insertSelfLastSeenSQL, new Object[]{nodeId}, new int[]{Types.VARCHAR});
			if (affected != 1) {
				logger.warn("expected to see exactly 1 record affected while registering " + nodeId + ", yet actual result is " + affected);
			} else {
				logger.info("registration of active node " + nodeId + " succeeded");
			}
		}
	}

	@Override
	public int removeLongTimeNoSeeNodes(long maxTimeNoSeeMillis) {
		try {
			return getJdbcTemplate().update(removeLongTimeNoSeeSQL, new Object[]{maxTimeNoSeeMillis}, new int[]{Types.BIGINT});
		} catch (DataAccessException dae) {
			throw new CtsGeneralFailure("failed while looking up and removing non-active nodes", dae);
		}
	}

	private Set<String> getCTSTableNames() {
		return Stream.of(ACTIVE_NODES_TABLE_NAME, META_TABLE_NAME, BODY_TABLE_NAME + "0", BODY_TABLE_NAME + "1", BODY_TABLE_NAME + "2", BODY_TABLE_NAME + "3").collect(Collectors.toSet());
	}

	private Set<String> getCTSIndexNames() {
		return Stream.of("CTSKM_PK", "CTSKM_IDX_2", "CTSKM_IDX_5", "CTSKM_IDX_6", "CTSKB_PK_P0", "CTSKB_PK_P1", "CTSKB_PK_P2", "CTSKB_PK_P3").collect(Collectors.toSet());
	}

	private Set<String> getCTSSequenceNames() {
		return Stream.of(CLUSTER_TASK_ID_SEQUENCE).collect(Collectors.toSet());
	}
}
