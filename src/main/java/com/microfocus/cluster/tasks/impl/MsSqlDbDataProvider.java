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
import com.microfocus.cluster.tasks.api.enums.ClusterTaskInsertStatus;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskStatus;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskType;
import com.microfocus.cluster.tasks.api.errors.CtsGeneralFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by gullery on 12/04/2018.
 * <p>
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
	private final String updateScheduledTaskIntervalSQL;

	private final String takeLockForSelectForRunTasksSQL;
	private final Map<Integer, String> selectForUpdateTasksSQLs = new HashMap<>();
	private final Map<Long, String> selectTaskBodyByPartitionSQLs = new HashMap<>();
	private final String updateTasksStartedSQL;
	private final String releaseLockForSelectForRunTasksSQL;

	private final String takeLockForSelectForCleanTasksSQL;
	private final String selectReRunnableStaledTasksSQL;
	private final String releaseLockForSelectForCleanTasksSQL;

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

		//  insert / update tasks
		String insertFields = String.join(",", META_ID, TASK_TYPE, PROCESSOR_TYPE, UNIQUENESS_KEY, CONCURRENCY_KEY, APPLICATION_KEY, DELAY_BY_MILLIS, BODY_PARTITION, ORDERING_FACTOR, CREATED, STATUS);
		insertTaskWithoutBodySQL = "INSERT INTO " + META_TABLE_NAME + " (" + insertFields + ")" +
				" VALUES (NEXT VALUE FOR " + CLUSTER_TASK_ID_SEQUENCE + ", ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CAST(FORMAT(SYSDATETIME(),'yyMMddHHmmssfffffff') AS BIGINT) + ?), GETDATE(), " + ClusterTaskStatus.PENDING.value + ")";
		updateScheduledTaskIntervalSQL = "UPDATE " + META_TABLE_NAME +
				" SET " + CREATED + " = GETDATE(), " + DELAY_BY_MILLIS + " = ?" +
				" WHERE " + PROCESSOR_TYPE + " = ? AND " + TASK_TYPE + " = " + ClusterTaskType.SCHEDULED.value + " AND " + STATUS + " = " + ClusterTaskStatus.PENDING.value;

		//  select and run tasks flow
		takeLockForSelectForRunTasksSQL = "BEGIN TRAN; EXEC sp_getapplock @Resource = 'LOCK_FOR_TASKS_DISPATCH', @LockMode = 'Exclusive', @LockOwner = 'Transaction'";
		String selectFields = String.join(",", META_ID, TASK_TYPE, PROCESSOR_TYPE, UNIQUENESS_KEY, CONCURRENCY_KEY, APPLICATION_KEY, ORDERING_FACTOR, DELAY_BY_MILLIS, BODY_PARTITION, STATUS);
		for (int maxProcessorTypes : new Integer[]{20, 50, 100, 500}) {
			String processorTypesInParameter = String.join(",", Collections.nCopies(maxProcessorTypes, "?"));
			selectForUpdateTasksSQLs.put(maxProcessorTypes,
					"SELECT * FROM" +
							"   (SELECT " + selectFields + "," +
							"       ROW_NUMBER() OVER (PARTITION BY " + CONCURRENCY_KEY + " ORDER BY " + ORDERING_FACTOR + "," + META_ID + " ASC) AS row_index," +
							"       COUNT(CASE WHEN " + STATUS + " = " + ClusterTaskStatus.RUNNING.value + " THEN 1 ELSE NULL END) OVER (PARTITION BY " + CONCURRENCY_KEY + " ORDER BY " + ORDERING_FACTOR + "," + META_ID + " ASC) AS running_count" +
							"   FROM " + META_TABLE_NAME +
							"   WHERE " + PROCESSOR_TYPE + " IN(" + processorTypesInParameter + ")" +
							"       AND " + STATUS + " < " + ClusterTaskStatus.FINISHED.value +
							"       AND " + CREATED + " < DATEADD(MILLISECOND, -" + DELAY_BY_MILLIS + ", GETDATE())) meta" +
							" WHERE ((meta." + CONCURRENCY_KEY + " IS NOT NULL AND meta.row_index <= 1 AND meta.running_count = 0)" +
							"       OR (meta." + CONCURRENCY_KEY + " IS NULL AND meta." + STATUS + " = " + ClusterTaskStatus.PENDING.value + "))"
			);
		}
		for (long partition = 0; partition < PARTITIONS_NUMBER; partition++) {
			selectTaskBodyByPartitionSQLs.put(partition, "SELECT " + BODY + " FROM " + BODY_TABLE_NAME + partition +
					" WHERE " + BODY_ID + " = ?");
			insertTaskWithBodySQLs.put(partition, "DECLARE @taskId BIGINT = NEXT VALUE FOR " + CLUSTER_TASK_ID_SEQUENCE + ";" +
					" INSERT INTO " + BODY_TABLE_NAME + partition + " (" + BODY_ID + "," + BODY + ") VALUES (@taskId, ?);" +
					" INSERT INTO " + META_TABLE_NAME + " (" + insertFields + ")" +
					" VALUES (@taskId, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CAST(FORMAT(SYSDATETIME(),'yyMMddHHmmssfffffff') AS BIGINT) + ?), GETDATE(), " + ClusterTaskStatus.PENDING.value + ");"
			);
		}
		updateTasksStartedSQL = "UPDATE " + META_TABLE_NAME + " SET " + STATUS + " = " + ClusterTaskStatus.RUNNING.value + ", " + STARTED + " = GETDATE(), " + RUNTIME_INSTANCE + " = ?" +
				" WHERE " + META_ID + " = ?";
		releaseLockForSelectForRunTasksSQL = "EXEC sp_releaseapplock @Resource = 'LOCK_FOR_TASKS_DISPATCH', @LockOwner = 'Transaction'; COMMIT TRAN";

		//  clean up tasks flow
		takeLockForSelectForCleanTasksSQL = "BEGIN TRAN; EXEC sp_getapplock @Resource = 'LOCK_FOR_TASKS_GC', @LockMode = 'Exclusive', @LockOwner = 'Transaction'";
		String selectedForGCFields = String.join(",", META_ID, BODY_PARTITION, TASK_TYPE, PROCESSOR_TYPE, DELAY_BY_MILLIS, STATUS);
		selectReRunnableStaledTasksSQL = "SELECT " + selectedForGCFields + " FROM " + META_TABLE_NAME +
				" WHERE " + TASK_TYPE + " = " + ClusterTaskType.SCHEDULED.value +
				"   AND " + RUNTIME_INSTANCE + " IS NOT NULL" +
				"   AND NOT EXISTS (SELECT 1 FROM " + ACTIVE_NODES_TABLE_NAME + " WHERE " + ACTIVE_NODE_ID + " = " + RUNTIME_INSTANCE + ")";
		releaseLockForSelectForCleanTasksSQL = "EXEC sp_releaseapplock @Resource = 'LOCK_FOR_TASKS_GC', @LockOwner = 'Transaction'; COMMIT TRAN";
	}

	@Override
	String[] getSelectReRunnableStaledTasksSQL() {
		return new String[]{takeLockForSelectForCleanTasksSQL, selectReRunnableStaledTasksSQL, releaseLockForSelectForCleanTasksSQL};
	}

	@Override
	String getUpdateScheduledTaskIntervalSQL() {
		return updateScheduledTaskIntervalSQL;
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
			Integer tablesCount = getJdbcTemplate().queryForObject(sql, (resultSet, index) -> resultSet.getInt("cts_tables_count"));
			if (tablesCount != null && tablesCount == tableNames.size()) {
				//  check indices existence
				Set<String> indexNames = getCTSIndexNames();
				sql = areIndicesReadySQL;
				Integer indicesCount = getJdbcTemplate().queryForObject(sql, (resultSet, index) -> resultSet.getInt("cts_indices_count"));
				if (indicesCount != null && indicesCount == indexNames.size()) {
					//  check sequences existence
					Set<String> sequenceNames = getCTSSequenceNames();
					sql = areSequencesReadySQL;
					Integer sequencesCount = getJdbcTemplate().queryForObject(sql, (resultSet, index) -> resultSet.getInt("cts_sequences_count"));
					if (sequencesCount != null && sequencesCount == sequenceNames.size()) {
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
	public ClusterTaskPersistenceResult[] storeTasks(ClusterTaskImpl... tasks) {
		List<ClusterTaskPersistenceResult> result = new ArrayList<>(tasks.length);

		for (ClusterTaskImpl task : tasks) {
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
								task.applicationKey,
								task.delayByMillis,
								task.partitionIndex,
								task.orderingFactor,
								task.delayByMillis
						};
						paramTypes = new int[]{
								Types.CLOB,                 //  task body -  will be used only if actually has body
								Types.BIGINT,               //  task type
								Types.NVARCHAR,             //  processor type
								Types.NVARCHAR,             //  uniqueness key
								Types.NVARCHAR,             //  concurrency key
								Types.NVARCHAR,             //  application key
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
								task.applicationKey,
								task.delayByMillis,
								task.partitionIndex,
								task.orderingFactor,
								task.delayByMillis
						};
						paramTypes = new int[]{
								Types.BIGINT,               //  task type
								Types.NVARCHAR,             //  processor type
								Types.NVARCHAR,             //  uniqueness key
								Types.NVARCHAR,             //  concurrency key
								Types.NVARCHAR,             //  application key
								Types.BIGINT,               //  delay by millis
								Types.BIGINT,               //  partition index
								Types.BIGINT,               //  ordering factor
								Types.BIGINT                //  delay by millis (second time for potential ordering calculation based on creation time when ordering is NULL)
						};
					}

					//  insert task
					jdbcTemplate.update(insertTaskSql, paramValues, paramTypes);
					result.add(new ClusterTaskPersistenceResultImpl(ClusterTaskInsertStatus.SUCCESS));
					if (logger.isDebugEnabled()) {
						logger.debug("successfully created " + task);
					}
				} catch (DuplicateKeyException dke) {
					transactionStatus.setRollbackOnly();
					result.add(new ClusterTaskPersistenceResultImpl(ClusterTaskInsertStatus.UNIQUE_CONSTRAINT_FAILURE));
					logger.info(clusterTasksService.getInstanceID() + " rejected " + task + " due to uniqueness violation; specifically: " + dke.getMostSpecificCause().getMessage());
				} catch (Exception e) {
					transactionStatus.setRollbackOnly();
					result.add(new ClusterTaskPersistenceResultImpl(ClusterTaskInsertStatus.UNEXPECTED_FAILURE));
					logger.error(clusterTasksService.getInstanceID() + " failed to persist " + task, e);
				}
				return null;
			});
		}

		return result.toArray(new ClusterTaskPersistenceResult[0]);
	}

	@Override
	public void retrieveAndDispatchTasks(Map<String, ClusterTasksProcessorBase> availableProcessors) {
		Map<ClusterTasksProcessorBase, Collection<ClusterTaskImpl>> tasksToRun = new HashMap<>();
		JdbcTemplate jdbcTemplate = getJdbcTemplate();

		//  within the same transaction do:
		//  - SELECT candidate tasks to be run
		//  - LET processors to pick up the tasks that will actually run
		//  - UPDATE those tasks as RUNNING
		getTransactionTemplate().execute(transactionStatus -> {
			try {
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

				List<ClusterTaskImpl> tasks;
				jdbcTemplate.execute(takeLockForSelectForRunTasksSQL);
				tasks = jdbcTemplate.query(sql, params, paramTypes, this::tasksMetadataReader);
				if (tasks != null && !tasks.isEmpty()) {
					Map<String, List<ClusterTaskImpl>> tasksByProcessor = tasks.stream().collect(Collectors.groupingBy(ti -> ti.processorType));
					Set<Long> tasksToRunIDs = new HashSet<>();

					//  let processors decide which tasks will be processed from all available
					tasksByProcessor.forEach((processorType, processorTasks) -> {
						ClusterTasksProcessorBase processor = availableProcessors.get(processorType);
						Collection<ClusterTaskImpl> tmpTasks = processor.selectTasksToRun(processorTasks);
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
						int[] updateResults = jdbcTemplate.batchUpdate(updateTasksStartedSQL, updateParams, new int[]{Types.NVARCHAR, Types.BIGINT});
						if (logger.isDebugEnabled()) {
							logger.debug("update tasks to RUNNING results: " + Stream.of(updateResults).map(String::valueOf).collect(Collectors.joining(", ")));
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
			} finally {
				jdbcTemplate.execute(releaseLockForSelectForRunTasksSQL);
			}

			return null;
		});

		//  actually deliver tasks to processors
		tasksToRun.forEach((processor, tasks) -> processor.handleTasks(tasks, this));
	}

	@Override
	public String retrieveTaskBody(Long taskId, Long partitionIndex) {
		try {
			JdbcTemplate jdbcTemplate = getJdbcTemplate();
			String sql = selectTaskBodyByPartitionSQLs.get(partitionIndex);
			return jdbcTemplate.query(
					sql,
					new Object[]{taskId},
					new int[]{Types.BIGINT},
					this::rowToTaskBodyReader);
		} catch (Exception e) {
			throw new CtsGeneralFailure(clusterTasksService.getInstanceID() + " failed to retrieve task's body", e);
		}
	}

	@Override
	public void updateSelfLastSeen(String nodeId) {
		int updated = getJdbcTemplate().update(updateSelfLastSeenSQL, new Object[]{nodeId}, new int[]{Types.NVARCHAR});
		if (updated == 0) {
			logger.info("node " + nodeId + " activity was NOT UPDATED, performing initial registration...");
			int affected = getJdbcTemplate().update(insertSelfLastSeenSQL, new Object[]{nodeId}, new int[]{Types.NVARCHAR});
			if (affected != 1) {
				logger.warn("expected to see exactly 1 record affected while registering " + nodeId + ", yet actual result is " + affected);
			} else {
				logger.info("registration of active node " + nodeId + " succeeded");
			}
		}
	}

	@Override
	public int removeLongTimeNoSeeNodes(long maxTimeNoSeeMillis) {
		return getJdbcTemplate().update(removeLongTimeNoSeeSQL, new Object[]{maxTimeNoSeeMillis}, new int[]{Types.BIGINT});
	}

	private Set<String> getCTSTableNames() {
		return Stream.of(ACTIVE_NODES_TABLE_NAME, META_TABLE_NAME, BODY_TABLE_NAME + "0", BODY_TABLE_NAME + "1", BODY_TABLE_NAME + "2", BODY_TABLE_NAME + "3").collect(Collectors.toSet());
	}

	private Set<String> getCTSIndexNames() {
		return Stream.of("CTSAN_PK", "CTSKM_PK", "CTSKM_IDX_2", "CTSKM_IDX_5", "CTSKM_IDX_6", "CTSKB_PK_P0", "CTSKB_PK_P1", "CTSKB_PK_P2", "CTSKB_PK_P3").collect(Collectors.toSet());
	}

	private Set<String> getCTSSequenceNames() {
		return Stream.of(CLUSTER_TASK_ID_SEQUENCE).collect(Collectors.toSet());
	}
}
