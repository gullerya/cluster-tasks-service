package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.CTPPersistStatus;
import com.microfocus.octane.cluster.tasks.api.ClusterTaskStatus;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.microfocus.octane.cluster.tasks.api.CtsGeneralFailure;
import com.microfocus.octane.cluster.tasks.api.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.ClusterTaskPersistenceResult;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorDefault;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;
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
import java.util.UUID;
import java.util.stream.Collectors;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI.DBType;

import javax.sql.DataSource;

import static java.sql.Types.BIGINT;

/**
 * Created by gullery on 08/05/2016.
 * <p>
 * Cluster tasks data provider backed by DB
 */

@Component
class ClusterTasksDbDataProvider implements ClusterTasksDataProvider {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksDbDataProvider.class);
	private static final String RUNTIME_INSTANCE_ID = UUID.randomUUID().toString();
	private static final int PARTITIONS_NUMBER = 4;
	private static final long MAX_TIME_TO_RUN_DEFAULT = 1000 * 60;
	private ZonedDateTime lastTruncateTime;
	private JdbcTemplate jdbcTemplate;
	private TransactionTemplate transactionTemplate;

	@Autowired
	private ClusterTasksServiceConfigurerSPI serviceConfigurer;
	@Autowired
	private ClusterTaskWorkersFactory clusterTaskWorkersFactory;

	@Override
	public ClusterTasksDataProviderType getType() {
		return ClusterTasksDataProviderType.DB;
	}

	//  TODO: support bulk insert here
	@Override
	public ClusterTaskPersistenceResult[] storeTasks(String processorType, ClusterTask... tasks) {
		if (processorType == null || processorType.isEmpty()) {
			throw new IllegalArgumentException("processor type MUST NOT be null nor empty");
		}
		if (tasks == null || tasks.length == 0) {
			throw new IllegalArgumentException("tasks MUST NOT be null nor empty");
		}

		List<ClusterTaskPersistenceResult> result = new ArrayList<>(tasks.length);

		for (ClusterTask originalTask : tasks) {
			CTPPersistStatus validationStatus = validateAndNormalizeTask(originalTask);

			if (validationStatus == CTPPersistStatus.SUCCESS) {
				getTransactionTemplate().execute(transactionStatus -> {
					ClusterTask task = new ClusterTask(originalTask);
					try {
						JdbcTemplate jdbcTemplate = getJdbcTemplate();
						boolean hasBody = false;

						//  pre-process values
						task.setProcessorType(processorType);
						if (task.getTaskType() == null) {
							task.setTaskType(ClusterTaskType.REGULAR);
						}
						if (task.getBody() != null) {
							task.setPartitionIndex(resolveBodyTablePartitionIndex());
							hasBody = true;
						}

						//  insert task
						String insertTaskSql = ClusterTasksDbUtils.buildInsertTaskSQL(serviceConfigurer.getDbType(), task.getPartitionIndex());
						Object[] paramValues = new Object[]{
								task.getTaskType().value,
								task.getProcessorType(),
								task.getUniquenessKey(),
								task.getConcurrencyKey(),
								task.getDelayByMillis(),
								task.getMaxTimeToRunMillis(),
								task.getPartitionIndex(),
								task.getOrderingFactor(),
								task.getDelayByMillis(),
								task.getBody()
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

						result.add(new ClusterTaskPersistenceResult(CTPPersistStatus.SUCCESS));
						logger.debug("successfully created " + task);
					} catch (DuplicateKeyException dke) {
						transactionStatus.setRollbackOnly();
						result.add(new ClusterTaskPersistenceResult(CTPPersistStatus.UNIQUE_CONSTRAINT_FAILURE));
						logger.info("rejected " + task + " due to uniqueness violation");
					} catch (Exception e) {
						transactionStatus.setRollbackOnly();
						result.add(new ClusterTaskPersistenceResult(CTPPersistStatus.UNEXPECTED_FAILURE));
						logger.error("failed to persist " + task, e);
					}
					return null;
				});
			} else {
				result.add(new ClusterTaskPersistenceResult(validationStatus));
				logger.debug("rejected invalid " + originalTask);
			}
		}

		return result.toArray(new ClusterTaskPersistenceResult[result.size()]);
	}

	@Override
	public void retrieveAndDispatchTasks(Map<String, ClusterTasksProcessorDefault> availableProcessors) {
		getTransactionTemplate().execute(transactionStatus -> {
			try {
				JdbcTemplate jdbcTemplate = getJdbcTemplate();
				String selectForUpdateSql = ClusterTasksDbUtils.buildSelectForUpdateTasksSQL(getDBType(), 500);
				String[] availableProcessorTypes = availableProcessors.keySet().toArray(new String[availableProcessors.size()]);
				int paramsTotal = 500;

				//  prepare params
				Object[] params = new Object[paramsTotal];
				System.arraycopy(availableProcessorTypes, 0, params, 0, availableProcessorTypes.length);
				for (int i = availableProcessorTypes.length; i < paramsTotal; i++) params[i] = null;

				//  prepare param types
				int[] paramTypes = new int[paramsTotal];
				for (int i = 0; i < paramsTotal; i++) paramTypes[i] = Types.NVARCHAR;

				List<ClusterTask> tasks = jdbcTemplate.query(selectForUpdateSql, params, paramTypes, ClusterTasksDbUtils::tasksMetadataReader);
				if (!tasks.isEmpty()) {
					List<Long> startedTasksIDs = new LinkedList<>();

					//  dispatch tasks where relevant
					//
					tasks.forEach(task -> {
						ClusterTasksProcessorDefault processor = availableProcessors.get(task.getProcessorType());
						if (processor != null && processor.isReadyToHandleTaskInternal()) {
							try {
								logger.debug("handing out " + task);
								ClusterTasksWorker worker = clusterTaskWorkersFactory.createWorker(this, processor, task);
								processor.internalProcessTasksAsync(worker);
								startedTasksIDs.add(task.getId());
							} catch (Exception e) {
								logger.error("failed to hand out " + task + " to processor " + processor.getType(), e);
							} finally {
								logger.debug("finished handing out " + task);
							}
						}
					});

					//  update started tasks in DB within the same transaction
					//
					if (!startedTasksIDs.isEmpty()) {
						try {
							String updateTasksStartedSQL = ClusterTasksDbUtils.buildUpdateTaskStartedSQL(serviceConfigurer.getDbType(), startedTasksIDs.size());
							Object[] updateParams = new Object[1 + startedTasksIDs.size()];
							int[] updateParamTypes = new int[1 + startedTasksIDs.size()];
							updateParams[0] = RUNTIME_INSTANCE_ID;
							updateParamTypes[0] = Types.VARCHAR;
							for (int i = 0; i < startedTasksIDs.size(); i++) {
								updateParams[i + 1] = startedTasksIDs.get(i);
								updateParamTypes[i + 1] = BIGINT;
							}
							jdbcTemplate.update(updateTasksStartedSQL, updateParams, updateParamTypes);
						} catch (DataAccessException dae) {
							throw new CtsGeneralFailure("failed to update tasks to started", dae);
						}
						logger.debug("from a total of " + tasks.size() + " available tasks " + startedTasksIDs.size() + " has been started");
					} else {
						logger.warn("from a total of " + tasks.size() + " available tasks none has been started");
					}
				}
			} catch (Exception e) {
				logger.error("failed to retrieve/dispatch/start cluster task/s", e);
			}

			return null;
		});
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
	public void updateTaskFinished(Long taskId) {
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
	public void updateTaskReenqueued(Long taskId) {
		if (taskId == null) {
			throw new IllegalArgumentException("task ID MUST NOT be null");
		}

		try {
			JdbcTemplate jdbcTemplate = getJdbcTemplate();
			String updateTaskFinishedSQL = ClusterTasksDbUtils.buildUpdateTaskReenqueueSQL(1);
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
		getTransactionTemplate().execute(transactionStatus -> {
			try {
				JdbcTemplate jdbcTemplate = getJdbcTemplate();
				String selectGCValidTasksSQL = ClusterTasksDbUtils.buildSelectGCValidTasksSQL(getDBType());
				List<ClusterTask> gcCandidates = jdbcTemplate.query(selectGCValidTasksSQL, ClusterTasksDbUtils::gcCandidatesReader);

				//  delete garbage tasks data
				Map<Long, Long> dataSetToDelete = gcCandidates.stream()
						.filter(task -> task.getTaskType() == ClusterTaskType.REGULAR)
						.collect(Collectors.toMap(ClusterTask::getId, ClusterTask::getPartitionIndex));
				if (!dataSetToDelete.isEmpty()) {
					deleteGarbageTasksData(jdbcTemplate, dataSetToDelete);
				}

				//  update tasks valid for reenqueue
				List<Long> dataSetToUpdate = gcCandidates.stream()
						.filter(task -> task.getTaskType() == ClusterTaskType.SCHEDULED)
						.map(ClusterTask::getId)
						.collect(Collectors.toList());
				if (!dataSetToUpdate.isEmpty()) {
					updateReenqueueTasks(jdbcTemplate, dataSetToUpdate);
				}
			} catch (Exception e) {
				logger.error("failed to cleanup cluster tasks", e);
				throw new CtsGeneralFailure("failed to cleanup cluster tasks", e);
			}

			return null;
		});
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

	private CTPPersistStatus validateAndNormalizeTask(ClusterTask task) {
		CTPPersistStatus result = CTPPersistStatus.SUCCESS;
		//  validation
		//  TODO: add more task validations here...
		if (task == null) {
			result = CTPPersistStatus.NULL_TASK_FAILURE;
		} else if (task.getUniquenessKey() != null && task.getUniquenessKey().length() > 40) {
			result = CTPPersistStatus.UNIQUENESS_KEY_TOO_LONG_FAILURE;
		} else if (task.getConcurrencyKey() != null && task.getConcurrencyKey().length() > 40) {
			result = CTPPersistStatus.CONCURRENCY_KEY_TOO_LONG_FAILURE;
		} else {
			//  normalization
			if (task.getUniquenessKey() == null) {
				task.setUniquenessKey(UUID.randomUUID().toString());
			}
			if (task.getDelayByMillis() == null) {
				task.setDelayByMillis(0L);
			}
			if (task.getMaxTimeToRunMillis() == null || task.getMaxTimeToRunMillis() == 0) {
				task.setMaxTimeToRunMillis(MAX_TIME_TO_RUN_DEFAULT);
			}
		}

		return result;
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
			int deleteBulkSize = 1000;
			int tPointer = 0;
			String deleteTimedoutMetaSQL = ClusterTasksDbUtils.buildDeleteTaskMetaSQL(deleteBulkSize);
			Object[] params = new Object[deleteBulkSize];

			//  prepare param types
			int[] paramTypes = new int[deleteBulkSize];
			for (int i = 0; i < paramTypes.length; i++) paramTypes[i] = BIGINT;

			//  delete metas
			Long[] taskIDs = taskIDsBodyPartitionsMap.keySet().toArray(new Long[taskIDsBodyPartitionsMap.size()]);
			while (tPointer < taskIDs.length) {
				int subListSize = Math.min(deleteBulkSize, taskIDs.length - tPointer);
				System.arraycopy(taskIDs, tPointer, params, 0, subListSize);
				for (int i = subListSize; i < deleteBulkSize; i++) params[i] = null;
				int deletedMetas = jdbcTemplate.update(deleteTimedoutMetaSQL, params, paramTypes);
				logger.debug("deleted " + deletedMetas + " task/s (metadata)");
				tPointer += deleteBulkSize;
			}

			//  delete bodies
			Map<Long, Set<Long>> partitionsToIdsMap = new LinkedHashMap<>();
			taskIDsBodyPartitionsMap.forEach((taskId, partitionIndex) -> partitionsToIdsMap
					.computeIfAbsent(partitionIndex, pId -> new LinkedHashSet<>())
					.add(taskId));
			partitionsToIdsMap.forEach((partitionId, taskIDsInPartition) -> {
				int bPointer = 0;
				String deleteTimedoutBodySQL = ClusterTasksDbUtils.buildDeleteTaskBodySQL(partitionId, deleteBulkSize);

				Long[] itemIDs = taskIDsInPartition.toArray(new Long[taskIDsInPartition.size()]);
				while (bPointer < itemIDs.length) {
					int subListSize = Math.min(deleteBulkSize, itemIDs.length - bPointer);
					System.arraycopy(itemIDs, bPointer, params, 0, subListSize);
					for (int i = subListSize; i < deleteBulkSize; i++) params[i] = null;
					int deletedBodies = jdbcTemplate.update(deleteTimedoutBodySQL, params, paramTypes);
					logger.debug("deleted " + deletedBodies + " task/s (content)");
					bPointer += deleteBulkSize;
				}
			});

			//  truncate currently non-active body tables (that are safe to truncate)
			checkAndTruncateBodyTables();
		} catch (Exception e) {
			logger.error("failed to delete Garbage tasks data", e);
		}
	}

	private void updateReenqueueTasks(JdbcTemplate jdbcTemplate, List<Long> taskIDsToReenqueue) {
		try {
			String updateReenqueueTasks = ClusterTasksDbUtils.buildUpdateTaskReenqueueSQL(taskIDsToReenqueue.size());
			Object[] params = new Object[taskIDsToReenqueue.size()];
			int[] paramTypes = new int[taskIDsToReenqueue.size()];
			for (int i = 0; i < taskIDsToReenqueue.size(); i++) {
				params[i] = taskIDsToReenqueue.get(i);
				paramTypes[i] = BIGINT;
			}
			jdbcTemplate.update(updateReenqueueTasks, params, paramTypes);
		} catch (Exception e) {
			logger.error("failed to update Reenqueue tasks data", e);
		}
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
