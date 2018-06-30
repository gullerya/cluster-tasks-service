package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskStatus;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskType;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksService;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.microfocus.octane.cluster.tasks.api.errors.CtsSqlFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
	static final String MAX_TIME_TO_RUN = META_COLUMNS_PREFIX.concat("MAX_TIME_TO_RUN");
	static final String BODY_PARTITION = META_COLUMNS_PREFIX.concat("BODY_PARTITION");

	//  Content table
	private static final String BODY_COLUMNS_PREFIX = "CTSKB_";
	static final String BODY_TABLE_NAME = "CLUSTER_TASK_BODY_P";
	static final String BODY_ID = BODY_COLUMNS_PREFIX.concat("ID");
	static final String BODY = BODY_COLUMNS_PREFIX.concat("BODY");

	final int PARTITIONS_NUMBER = 4;

	private final Map<Long, String> lookupOrphansByPartitionSQLs = new LinkedHashMap<>();
	private final String deleteTaskMetaSQL;
	private final Map<Long, String> deleteTaskBodyByPartitionSQLs = new LinkedHashMap<>();
	private final Map<Long, String> truncateByPartitionSQLs = new LinkedHashMap<>();

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
		deleteTaskMetaSQL = "DELETE FROM " + META_TABLE_NAME + " WHERE " + META_ID + " = ?";
		for (long partition = 0; partition < PARTITIONS_NUMBER; partition++) {
			lookupOrphansByPartitionSQLs.put(partition, "SELECT " + String.join(",", BODY_ID, BODY, META_ID) + " FROM " + BODY_TABLE_NAME + partition +
					" LEFT OUTER JOIN " + META_TABLE_NAME + " ON " + META_ID + " = " + BODY_ID);
			deleteTaskBodyByPartitionSQLs.put(partition, "DELETE FROM " + BODY_TABLE_NAME + partition +
					" WHERE " + BODY_ID + " = ?");
			truncateByPartitionSQLs.put(partition, "TRUNCATE TABLE " + BODY_TABLE_NAME + partition);
		}
	}

	@Override
	public ClusterTasksDataProviderType getType() {
		return ClusterTasksDataProviderType.DB;
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

	List<TaskInternal> tasksMetadataReader(ResultSet resultSet) {
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

	String rowToTaskBodyReader(ResultSet resultSet) {
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

	Map<String, Integer> scheduledPendingReader(ResultSet resultSet) {
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

	List<TaskInternal> gcCandidatesReader(ResultSet resultSet) {
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

	long resolveBodyTablePartitionIndex() {
		int hour = ZonedDateTime.now(ZoneOffset.UTC).getHour();
		return hour / (24 / PARTITIONS_NUMBER);
	}

	void deleteGarbageTasksData(JdbcTemplate jdbcTemplate, Map<Long, Long> taskIDsBodyPartitionsMap) {
		try {
			//  delete metas
			String deleteTimedoutMetaSQL = buildDeleteTaskMetaSQL();
			List<Object[]> mParams = taskIDsBodyPartitionsMap.keySet().stream()
					.sorted()
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
			String findAnyRowsInBodySQL = buildSelectVerifyBodyTableSQL(partitionIndex);
			String truncateBodyTableSQL = buildTruncateBodyTableSQL(partitionIndex);

			BodyTablePreTruncateVerificationResult verificationResult = jdbcTemplate.query(findAnyRowsInBodySQL, this::rowsToIDsInPartitionReader);
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

	private String buildDeleteTaskMetaSQL() {
		return deleteTaskMetaSQL;
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
