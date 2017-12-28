package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.enums.CTPPersistStatus;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskStatus;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskType;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksService;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.microfocus.octane.cluster.tasks.api.errors.CtsGeneralFailure;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Types;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI.DBType;

import javax.sql.DataSource;

import static java.sql.Types.BIGINT;
import static java.sql.Types.VARCHAR;

/**
 * Created by gullery on 08/05/2016.
 * <p>
 * Cluster tasks data provider backed by DB
 */

class ClusterTasksDbDataProvider implements ClusterTasksDataProvider {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksDbDataProvider.class);
	private final int PARTITIONS_NUMBER = 4;
	private ZonedDateTime lastTruncateTime;
	private JdbcTemplate jdbcTemplate;
	private TransactionTemplate transactionTemplate;

	@Autowired
	private ClusterTasksService clusterTasksService;
	@Autowired
	private ClusterTasksServiceConfigurerSPI serviceConfigurer;

	@Override
	public ClusterTasksDataProviderType getType() {
		return ClusterTasksDataProviderType.DB;
	}

	//  TODO: support bulk insert here
	@Override
	public ClusterTaskPersistenceResult[] storeTasks(TaskInternal... tasks) {
		if (!clusterTasksService.getReadyPromise().isDone()) {
			throw new IllegalStateException("cluster tasks service has not yet been initialized; either postpone tasks submission or listen to completion of [clusterTasksService].getReadyPromise()");
		}
		if (clusterTasksService.getReadyPromise().isCompletedExceptionally()) {
			throw new IllegalStateException("cluster tasks service failed to initialize; check prior logs for a root cause");
		}
		if (tasks == null || tasks.length == 0) {
			throw new IllegalArgumentException("tasks MUST NOT be null nor empty");
		}

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
					String insertTaskSql = ClusterTasksDbUtils.buildInsertTaskSQL(serviceConfigurer.getDbType(), task.partitionIndex);
					Object[] paramValues = new Object[]{
							task.taskType.getValue(),
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
					logger.info("rejected " + task + " due to uniqueness violation; specifically: " + dke.getMostSpecificCause().getMessage());
				} catch (Exception e) {
					transactionStatus.setRollbackOnly();
					result.add(new ClusterTaskPersistenceResultImpl(CTPPersistStatus.UNEXPECTED_FAILURE));
					logger.error("failed to persist " + task, e);
				}
				return null;
			});
		}

		return result.toArray(new ClusterTaskPersistenceResult[result.size()]);
	}

	@Override
	public void retrieveAndDispatchTasks(Map<String, ClusterTasksProcessorBase> availableProcessors) {
		Map<ClusterTasksProcessorBase, List<TaskInternal>> tasksToRun = new LinkedHashMap<>();

		//  within the same transaction do:
		//  - SELECT candidate tasks to be run
		//  - LET processors to pick up the tasks that will actually run
		//  - UPDATE those tasks as RUNNING
		getTransactionTemplate().execute(transactionStatus -> {
			JdbcTemplate jdbcTemplate = getJdbcTemplate();
			String selectForUpdateSql = ClusterTasksDbUtils.buildSelectForUpdateTasksSQL(getDBType(), 500);
			String[] availableProcessorTypes = availableProcessors.keySet().toArray(new String[availableProcessors.size()]);
			int paramsTotal = 100;

			//  prepare params
			Object[] params = new Object[paramsTotal];
			System.arraycopy(availableProcessorTypes, 0, params, 0, availableProcessorTypes.length);
			for (int i = availableProcessorTypes.length; i < paramsTotal; i++) params[i] = null;

			//  prepare param types
			int[] paramTypes = new int[paramsTotal];
			for (int i = 0; i < paramsTotal; i++) paramTypes[i] = Types.NVARCHAR;

			List<TaskInternal> tasks;
			tasks = jdbcTemplate.query(selectForUpdateSql, params, paramTypes, ClusterTasksDbUtils::tasksMetadataReader);
			if (!tasks.isEmpty()) {
				Map<String, List<TaskInternal>> tasksByProcessor = tasks.stream().collect(Collectors.groupingBy(ti -> ti.processorType));
				Set<Long> tasksToRunIDs = new LinkedHashSet<>();

				//  let processors decide which tasks will be processed from all available
				tasksByProcessor.forEach((processorType, processorTasks) -> {
					ClusterTasksProcessorBase processor = availableProcessors.get(processorType);
					List<TaskInternal> tmpTasks = processor.selectTasksToRun(processorTasks);
					tasksToRun.put(processor, tmpTasks);
					tasksToRunIDs.addAll(tmpTasks.stream().map(task -> task.id).collect(Collectors.toList()));
				});

				//  update selected tasks to RUNNING
				if (!tasksToRunIDs.isEmpty()) {
					try {
						String updateTasksStartedSQL = ClusterTasksDbUtils.buildUpdateTaskStartedSQL(serviceConfigurer.getDbType());
						String runtimeInstanceID = clusterTasksService.getInstanceID();
						List<Object[]> updateParams = tasksToRunIDs.stream()
								.map(id -> new Object[]{runtimeInstanceID, id})
								.collect(Collectors.toList());
						int[] updateResults = jdbcTemplate.batchUpdate(updateTasksStartedSQL, updateParams, new int[]{VARCHAR, BIGINT});
						logger.debug("update tasks to RUNNING result: [" +
								String.join(", ", Stream.of(updateResults).map(String::valueOf).collect(Collectors.toList())) +
								"]");
					} catch (DataAccessException dae) {
						throw new CtsGeneralFailure("failed to update tasks to started", dae);
					}
					logger.debug("from a total of " + tasks.size() + " available tasks " + tasksToRunIDs.size() + " has been started");
				} else {
					logger.warn("from a total of " + tasks.size() + " available tasks none has been started");
				}
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
			String sql = ClusterTasksDbUtils.buildReadTaskBodySQL(partitionIndex);
			return jdbcTemplate.query(
					sql,
					new Object[]{taskId},
					new int[]{BIGINT},
					ClusterTasksDbUtils::rowToTaskBodyReader);
		} catch (DataAccessException dae) {
			logger.error("failed to retrieve task's body", dae);
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
			String updateTaskFinishedSQL = ClusterTasksDbUtils.buildUpdateTaskFinishedSQL();
			jdbcTemplate.update(
					updateTaskFinishedSQL,
					new Object[]{taskId},
					new int[]{BIGINT});
		} catch (DataAccessException dae) {
			logger.error("failed to update task finished", dae);
		}
	}

	@Override
	public void handleGarbageAndStaled() {
		List<TaskInternal> dataSetToReschedule = new LinkedList<>();
		getTransactionTemplate().execute(transactionStatus -> {
			try {
				JdbcTemplate jdbcTemplate = getJdbcTemplate();
				String selectGCValidTasksSQL = ClusterTasksDbUtils.buildSelectGCValidTasksSQL(getDBType());
				List<TaskInternal> gcCandidates = jdbcTemplate.query(selectGCValidTasksSQL, ClusterTasksDbUtils::gcCandidatesReader);

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
				logger.error("failed to cleanup cluster tasks", e);
				throw new CtsGeneralFailure("failed to cleanup cluster tasks", e);
			}

			return null;
		});

		//  reschedule tasks of SCHEDULED type
		if (!dataSetToReschedule.isEmpty()) {
			rescheduleTasks(dataSetToReschedule);
		}
	}

	@Override
	public int countTasks(String processorType, Set<ClusterTaskStatus> statuses) {
		String countTasksSQL = ClusterTasksDbUtils.buildCountTasksSQL(processorType, statuses);
		return getJdbcTemplate().queryForObject(countTasksSQL, Integer.class);
	}

	@Override
	public int countTasks(String processorType, String concurrencyKey, Set<ClusterTaskStatus> statuses) {
		String countTasksSQL = ClusterTasksDbUtils.buildCountTasksSQL(processorType, concurrencyKey, statuses);
		return getJdbcTemplate().queryForObject(countTasksSQL, Integer.class);
	}

	private DBType getDBType() {
		DBType result;
		try {
			result = serviceConfigurer.getDbType();
			if (result == null) {
				throw new IllegalStateException("hosting application's configurer failed to provide valid DB type");
			}
		} catch (Exception e) {
			throw new IllegalStateException("hosting application's configurer failed to provide valid DB type", e);
		}
		return result;
	}

	private JdbcTemplate getJdbcTemplate() {
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

	private TransactionTemplate getTransactionTemplate() {
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

	private long resolveBodyTablePartitionIndex() {
		int hour = ZonedDateTime.now(ZoneOffset.UTC).getHour();
		return hour / (24 / PARTITIONS_NUMBER);
	}

	private void deleteGarbageTasksData(JdbcTemplate jdbcTemplate, Map<Long, Long> taskIDsBodyPartitionsMap) {
		try {
			//  delete metas
			String deleteTimedoutMetaSQL = ClusterTasksDbUtils.buildDeleteTaskMetaSQL();
			List<Object[]> mParams = taskIDsBodyPartitionsMap.keySet().stream()
					.map(id -> new Object[]{id})
					.collect(Collectors.toList());
			int[] deletedMetas = jdbcTemplate.batchUpdate(deleteTimedoutMetaSQL, mParams, new int[]{BIGINT});
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
				String deleteTimedoutBodySQL = ClusterTasksDbUtils.buildDeleteTaskBodySQL(partitionId);
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

	private void rescheduleTasks(List<TaskInternal> tasksToReschedule) {
		tasksToReschedule.forEach(task -> {
			task.uniquenessKey = task.processorType;
			task.concurrencyKey = task.processorType;
			task.delayByMillis = 0L;
		});
		storeTasks(tasksToReschedule.toArray(new TaskInternal[tasksToReschedule.size()]));
	}

	private void checkAndTruncateBodyTables() {
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
					logger.info("starting truncation of partition " + partition + "...");
					tryTruncateBodyTable(partition);
				}
			}
			lastTruncateTime = now;
		}
	}

	private void tryTruncateBodyTable(long partitionIndex) {
		try {
			JdbcTemplate jdbcTemplate = getJdbcTemplate();
			String findAnyRowsInBodySQL = ClusterTasksDbUtils.buildSelectVerifyBodyTableSQL(partitionIndex);
			String truncateBodyTableSQL = ClusterTasksDbUtils.buildTruncateBodyTableSQL(partitionIndex);

			BodyTablePreTruncateVerificationResult verificationResult = jdbcTemplate.query(findAnyRowsInBodySQL, ClusterTasksDbUtils::rowsToIDsInPartitionReader);
			if (verificationResult.getEntries().isEmpty()) {
				logger.info("partition " + partitionIndex + " found empty, proceeding with truncate");
				jdbcTemplate.execute(truncateBodyTableSQL);
				logger.info("partition " + partitionIndex + " truncate done");
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
					logger.info("partition " + partitionIndex + " found non-empty, but all of it's entries considered 'zombies', proceeding with truncate");
					jdbcTemplate.execute(truncateBodyTableSQL);
					logger.info("partition " + partitionIndex + " truncate done");
				} else {
					logger.warn("partition " + partitionIndex + " found non-empty, and " + nonZombieBodies + " of it's entries are not 'zombies', will not truncate");
				}
			}
		} catch (Exception e) {
			logger.error("failed to truncate partition " + partitionIndex, e);
		}
	}
}
