package com.microfocus.octane.cluster.tasks.api;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.CompletableFuture;

public interface ClusterTasksServiceConfigurerSPI {
	int MINIMAL_POLL_INTERVAL = 703;
	int DEFAULT_POLL_INTERVAL = 1023;
	int MINIMAL_GC_INTERVAL = 7131;
	int DEFAULT_GC_INTERVAL = 13039;

	enum DBType {ORACLE, MSSQL}

	/**
	 * OOTB provided promise that MUST BE resolved by the hosting application in order to allow ClusterTasksService to run correctly
	 * - resolved with TRUE when the configuration ready and ClusterTasksService may start it's routine
	 * - resolved with FALSE when hosting application decided that it fails to provide ClusterTasksService it's required configuration (DB connectivity, for instance)
	 *
	 * @return promise on configuration readiness
	 */
	CompletableFuture<Boolean> getConfigReadyLatch();

	/**
	 * return interval in millis to breathe between the tasks polling requests
	 * if result is lower than minimum figure - the MINIMAL_POLL_INTERVAL will be used
	 * if result is NULL - the DEFAULT_POLL_INTERVAL will be used
	 *
	 * @return interval in millis or NULL (default value will be taken)
	 */
	Integer getTasksPollIntervalMillis();

	/**
	 * return interval in millis to breathe between the GC cycles
	 * if result is lower than minimum figure - the MINIMAL_GC_INTERVAL will be used
	 * if result is NULL - the DEFAULT_GC_INTERVAL will be used
	 *
	 * @return either number of millis to wait between intervals or NULL (default value will be taken)
	 */
	Integer getGCIntervalMillis();

	/**
	 * returns DB type that ClusterTasksService is working with
	 *
	 * @return db type; MUST NOT be null
	 */
	DBType getDbType();

	/**
	 * returns valid jdbc template
	 *
	 * @return jdbc template; MUST NOT be null
	 */
	JdbcTemplate getJdbcTemplate();

	/**
	 * returns valid transaction template
	 *
	 * @return transaction template; MUST NOT be null
	 */
	TransactionTemplate getTransactionalTemplate();

	/**
	 * returns valid ID to be used to mark the newly created task with
	 *
	 * @return task ID
	 */
	long obtainAvailableTaskID();
}
