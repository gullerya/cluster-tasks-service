package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksService;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.microfocus.octane.cluster.tasks.api.enums.CTPPersistStatus;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskStatus;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskType;
import com.microfocus.octane.cluster.tasks.api.errors.CtsGeneralFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
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
 * Oracle oriented data provider
 */

final class OracleDbDataProvider extends ClusterTasksDbDataProvider {
	private static final Logger logger = LoggerFactory.getLogger(OracleDbDataProvider.class);

	private final String areTablesReadySQL;
	private final String areIndicesReadySQL;
	private final String areSequencesReadySQL;

	private final String insertTaskWithoutBodySQL;
	private final Map<Long, String> insertTaskWithBodySQL = new LinkedHashMap<>();
	private final Map<Integer, String> selectForUpdateTasksSQLs = new LinkedHashMap<>();
	private final Map<Long, String> selectTaskBodyByPartitionSQLs = new LinkedHashMap<>();

	private final String updateTasksStartedSQL;
	private final String updateTaskFinishedSQL;

	private final String countScheduledPendingTasksSQL;
	private final String selectGCValidTasksSQL;

	OracleDbDataProvider(ClusterTasksService clusterTasksService, ClusterTasksServiceConfigurerSPI serviceConfigurer) {
		super(clusterTasksService, serviceConfigurer);

		//  prepare SQL statements
		areTablesReadySQL = "SELECT COUNT(*) AS cts_tables_count FROM user_tables WHERE table_name IN(" +
				String.join(",", getCTSTableNames().stream().map(tn -> "'" + tn + "'").collect(Collectors.toSet())) + ")";
		areIndicesReadySQL = "SELECT COUNT(*) AS cts_indices_count FROM user_indexes WHERE index_name IN(" +
				String.join(",", getCTSIndexNames().stream().map(in -> "'" + in + "'").collect(Collectors.toSet())) + ")";
		areSequencesReadySQL = "SELECT COUNT(*) AS cts_sequences_count FROM user_sequences WHERE sequence_name IN(" +
				String.join(",", getCTSSequenceNames().stream().map(sn -> "'" + sn + "'").collect(Collectors.toSet())) + ")";

		String insertFields = String.join(",", META_ID, TASK_TYPE, PROCESSOR_TYPE, UNIQUENESS_KEY, CONCURRENCY_KEY, DELAY_BY_MILLIS, MAX_TIME_TO_RUN, BODY_PARTITION, ORDERING_FACTOR, CREATED, STATUS);
		insertTaskWithoutBodySQL = "DECLARE taskId NUMBER(19) := " + CLUSTER_TASK_ID_SEQUENCE + ".NEXTVAL;" +
				" BEGIN" +
				"   INSERT INTO " + META_TABLE_NAME + " (" + insertFields + ")" +
				"   VALUES (taskId, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, TO_NUMBER(TO_CHAR(SYSTIMESTAMP,'yyyymmddhh24missff3')) + ?), SYSDATE, " + ClusterTaskStatus.PENDING.value + ");" +
				" END;";

		String selectForRunFields = String.join(",", META_ID, TASK_TYPE, PROCESSOR_TYPE, UNIQUENESS_KEY, CONCURRENCY_KEY, ORDERING_FACTOR, DELAY_BY_MILLIS, MAX_TIME_TO_RUN, BODY_PARTITION, STATUS);
		for (int maxProcessorTypes : new Integer[]{20, 50, 100, 500}) {
			String processorTypesInParameter = String.join(",", Collections.nCopies(maxProcessorTypes, "?"));
			selectForUpdateTasksSQLs.put(maxProcessorTypes, "SELECT " + selectForRunFields +
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
					" FOR UPDATE");
		}
		for (long partition = 0; partition < PARTITIONS_NUMBER; partition++) {
			selectTaskBodyByPartitionSQLs.put(partition, "SELECT " + BODY + " FROM " + BODY_TABLE_NAME + partition +
					" WHERE " + BODY_ID + " = ?");
			insertTaskWithBodySQL.put(partition, "DECLARE taskId NUMBER(19) := " + CLUSTER_TASK_ID_SEQUENCE + ".NEXTVAL;" +
					" BEGIN" +
					"   INSERT INTO " + META_TABLE_NAME + " (" + insertFields + ")" +
					"   VALUES (taskId, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, TO_NUMBER(TO_CHAR(SYSTIMESTAMP,'yyyymmddhh24missff3')) + ?), SYSDATE, " + ClusterTaskStatus.PENDING.value + ");" +
					"   INSERT INTO " + BODY_TABLE_NAME + partition + " (" + String.join(",", BODY_ID, BODY) + ") VALUES (taskId, ?);" +
					" END;");
		}

		updateTasksStartedSQL = "UPDATE " + META_TABLE_NAME + " SET " + STATUS + " = " + ClusterTaskStatus.RUNNING.value + ", " + STARTED + " = SYSDATE, " + RUNTIME_INSTANCE + " = ?" +
				" WHERE " + META_ID + " = ?";
		updateTaskFinishedSQL = "UPDATE " + META_TABLE_NAME + " SET " + String.join(",", STATUS + " = " + ClusterTaskStatus.FINISHED.value, UNIQUENESS_KEY + " = RAWTOHEX(SYS_GUID())") +
				" WHERE " + META_ID + " = ?";

		countScheduledPendingTasksSQL = "SELECT " + PROCESSOR_TYPE + ",COUNT(*) AS total FROM " + META_TABLE_NAME +
				" WHERE " + TASK_TYPE + " = " + ClusterTaskType.SCHEDULED.value + " AND " + STATUS + " = " + ClusterTaskStatus.PENDING.value + " GROUP BY " + PROCESSOR_TYPE;

		String selectedForGCFields = String.join(",", META_ID, BODY_PARTITION, TASK_TYPE, PROCESSOR_TYPE, STATUS, MAX_TIME_TO_RUN);
		selectGCValidTasksSQL = "SELECT " + selectedForGCFields + " FROM " + META_TABLE_NAME +
				" WHERE " + STATUS + " = " + ClusterTaskStatus.FINISHED.value +
				" OR (" + STATUS + " = " + ClusterTaskStatus.RUNNING.value + " AND " + STARTED + " < SYSDATE - NUMTODSINTERVAL(" + MAX_TIME_TO_RUN + " / 1000, 'SECOND'))" +
				" FOR UPDATE";
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
						logger.warn(dataProviderName + " found being NOT READY: expected number of sequences - " + sequenceNames.size() + ", found - " + indicesCount);
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

	//  TODO: support bulk insert here
	@Override
	public ClusterTaskPersistenceResult[] storeTasks(TaskInternal... tasks) {
		List<ClusterTaskPersistenceResult> result = new ArrayList<>(tasks.length);

		for (TaskInternal task : tasks) {
			getTransactionTemplate().execute(transactionStatus -> {
				try {
					JdbcTemplate jdbcTemplate = getJdbcTemplate();
					boolean hasBody = task.body != null;
					if (hasBody) {
						task.partitionIndex = resolveBodyTablePartitionIndex();
					}

					//  insert task
					String insertTaskSql = buildInsertTaskSQL(task.partitionIndex);
					Object[] paramValues = new Object[]{
							task.taskType.value,
							task.processorType,
							task.uniquenessKey,
							task.concurrencyKey,
							task.delayByMillis,
							task.maxTimeToRunMillis,
							task.partitionIndex,
							task.orderingFactor,
							task.delayByMillis,
							task.body
					};
					int[] paramTypes = new int[]{
							Types.BIGINT,               //  task type
							Types.VARCHAR,              //  processor type
							Types.VARCHAR,              //  uniqueness key
							Types.VARCHAR,              //  concurrency key
							Types.BIGINT,               //  delay by millis
							Types.BIGINT,               //  max time to run millis
							Types.BIGINT,               //  partition index
							Types.BIGINT,               //  ordering factor
							Types.BIGINT,               //  delay by millis (second time for potential ordering calculation based on creation time when ordering is NULL)
							Types.CLOB                  //  task body -  will be used only if actually has body
					};

					jdbcTemplate.update(
							insertTaskSql,
							hasBody ? paramValues : Arrays.copyOfRange(paramValues, 0, paramValues.length - 1),
							hasBody ? paramTypes : Arrays.copyOfRange(paramTypes, 0, paramTypes.length - 1)
					);

					result.add(new ClusterTaskPersistenceResultImpl(CTPPersistStatus.SUCCESS));
					logger.debug("successfully created " + task);
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
		Map<ClusterTasksProcessorBase, Collection<TaskInternal>> tasksToRun = new LinkedHashMap<>();

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
				for (int testedParamTotal : selectForUpdateTasksSQLs.keySet()) {
					if (testedParamTotal >= availableProcessors.size()) {
						paramsTotal = testedParamTotal;
						sql = selectForUpdateTasksSQLs.get(testedParamTotal);
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
					Set<Long> tasksToRunIDs = new LinkedHashSet<>();

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
							logger.debug("update tasks to RUNNING result: [" + String.join(", ", Stream.of(updateResults).map(String::valueOf).collect(Collectors.toList())) + "]");
							logger.debug("from a total of " + tasks.size() + " available tasks " + tasksToRunIDs.size() + " has been started");
						}
					} else {
						logger.warn("from a total of " + tasks.size() + " available tasks none has been started");
					}
				}
			} catch (Exception e) {
				transactionStatus.setRollbackOnly();
				tasksToRun.clear();
				throw new CtsGeneralFailure("failed to retrieve and execute tasks", e);
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
		} catch (DataAccessException dae) {
			logger.error(clusterTasksService.getInstanceID() + " failed to retrieve task's body", dae);
			throw new CtsGeneralFailure("failed to retrieve task's body", dae);
		}
	}

	@Override
	public void updateTaskToFinished(Long taskId) {
		if (taskId == null) {
			throw new IllegalArgumentException("task ID MUST NOT be null");
		}

		try {
			JdbcTemplate jdbcTemplate = getJdbcTemplate();
			jdbcTemplate.update(
					updateTaskFinishedSQL,
					new Object[]{taskId},
					new int[]{BIGINT});
		} catch (DataAccessException dae) {
			logger.error(clusterTasksService.getInstanceID() + " failed to update task finished", dae);
		}
	}

	@Override
	public void handleGarbageAndStaled() {
		List<TaskInternal> dataSetToReschedule = new LinkedList<>();
		getTransactionTemplate().execute(transactionStatus -> {
			try {
				JdbcTemplate jdbcTemplate = getJdbcTemplate();
				List<TaskInternal> gcCandidates = jdbcTemplate.query(selectGCValidTasksSQL, this::gcCandidatesReader);

				//  delete garbage tasks data
				Map<Long, Long> dataSetToDelete = new LinkedHashMap<>();
				gcCandidates.forEach(candidate -> dataSetToDelete.put(candidate.id, candidate.partitionIndex));
				if (!dataSetToDelete.isEmpty()) {
					deleteGarbageTasksData(jdbcTemplate, dataSetToDelete);
				}

				//  collect tasks for rescheduling
				dataSetToReschedule.addAll(gcCandidates.stream()
						.filter(task -> task.taskType == ClusterTaskType.SCHEDULED)
						.collect(Collectors.toMap(task -> task.processorType, Function.identity(), (t1, t2) -> t2))
						.values()
				);
			} catch (Exception e) {
				transactionStatus.setRollbackOnly();
				throw new CtsGeneralFailure("failed to cleanup cluster tasks", e);
			}

			return null;
		});

		//  reschedule tasks of SCHEDULED type
		if (!dataSetToReschedule.isEmpty()) {
			reinsertScheduledTasks(dataSetToReschedule);
		}
	}

	@Override
	public void reinsertScheduledTasks(List<TaskInternal> candidatesToReschedule) {
		Map<String, Integer> pendingCount = getJdbcTemplate().query(countScheduledPendingTasksSQL, this::scheduledPendingReader);
		List<TaskInternal> tasksToReschedule = new LinkedList<>();
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

	private String buildInsertTaskSQL(Long partitionIndex) {
		return partitionIndex == null ?
				insertTaskWithoutBodySQL :
				insertTaskWithBodySQL.get(partitionIndex);
	}
}
