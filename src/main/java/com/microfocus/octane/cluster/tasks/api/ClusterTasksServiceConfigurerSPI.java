package com.microfocus.octane.cluster.tasks.api;

import javax.sql.DataSource;
import java.util.concurrent.CompletableFuture;

public interface ClusterTasksServiceConfigurerSPI {
	int MINIMAL_POLL_INTERVAL = 703;
	int DEFAULT_POLL_INTERVAL = 1023;
	int MINIMAL_MAINTENANCE_INTERVAL = 7131;
	int DEFAULT_MAINTENANCE_INTERVAL = 17039;

	enum DBType {MSSQL, ORACLE, POSTGRESQL}

	/**
	 * MAY provide a promise, which resolving will notify ClusterTasksService that it may start its job
	 * - the promise, if provided, MUST be resolved
	 * - resolving to TRUE means the configuration is ready and ClusterTasksService may start its routine
	 * - resolving to FALSE means that application failed to provide required configuration (DB connectivity, for instance) - ClusterTasksService won't run
	 *
	 * @return promise on configuration readiness; if NULL is returned - CLusterTasksService will continue as if it was resolved to TRUE
	 */
	CompletableFuture<Boolean> getConfigReadyLatch();

	/**
	 * MUST provide data source to the DB, that the ClusterTasksService's tables reside in
	 *
	 * @return working data source
	 */
	DataSource getDataSource();

	/**
	 * MAY provide a data source to the DB, that the ClusterTasksService's tables reside in
	 * this data source, if provided, MUST be privileged enough to perform schema changes
	 *
	 * @return admin data source; if returns NULL - this specific instance will NOT perform schema management
	 */
	DataSource getAdministrativeDataSource();

	/**
	 * MUST provide DB type, that ClusterTasksService will work with
	 *
	 * @return db type; MUST NOT be NULL
	 */
	DBType getDbType();

	/**
	 * MAY provide interval (in millis) of breathing between the tasks polls
	 * if value is lower than minimum figure - the MINIMAL_POLL_INTERVAL will be used
	 *
	 * @return number of millis between tasks polls; if NULL is returned - DEFAULT_POLL_INTERVAL will be taken
	 */
	Integer getTasksPollIntervalMillis();

	/**
	 * MAY provide interval (in millis) of breathing between the GC cycles
	 * if value is lower than minimum figure - the MINIMAL_MAINTENANCE_INTERVAL will be used
	 *
	 * @return number of millis between maintenance cycles; if NULL is returned - DEFAULT_MAINTENANCE_INTERVAL will be taken
	 */
	Integer getMaintenanceIntervalMillis();
}
