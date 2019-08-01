package com.gullerya.cluster.tasks.api;

import javax.sql.DataSource;
import java.util.concurrent.CompletableFuture;

/**
 * Cluster Tasks Service configurer SPI definition
 * - hosting application MUST have one and only one Spring bean implementing this SPI per Spring context, that is using CTS
 * - this class is the only configuration provider for CTS
 */

public interface ClusterTasksServiceConfigurerSPI {

	enum DBType {MSSQL, ORACLE, POSTGRESQL}

	/**
	 * MAY provide a promise, which resolving will notify ClusterTasksService that it may start its job
	 * - the promise, if provided, MUST be resolved
	 * - resolving to TRUE means the configuration is ready and ClusterTasksService may start its routine
	 * - resolving to FALSE means that application failed to provide required configuration (DB connectivity, for instance) - ClusterTasksService won't run
	 *
	 * @return promise on configuration readiness; if NULL is returned - CLusterTasksService will continue as if it was resolved to TRUE
	 */
	default CompletableFuture<Boolean> getConfigReadyLatch() {
		return null;
	}

	/**
	 * MUST provide data source to the DB, that the ClusterTasksService's tables reside in
	 *
	 * @return working data source; MUST NOT be NULL
	 */
	DataSource getDataSource();

	/**
	 * MAY provide a data source to the DB, that the ClusterTasksService's tables reside in
	 * this data source, if provided, MUST be privileged enough to perform schema changes
	 *
	 * @return admin data source; if returns NULL - this specific instance will NOT perform schema management
	 */
	default DataSource getAdministrativeDataSource() {
		return null;
	}

	/**
	 * MUST provide DB type, that ClusterTasksService will work with
	 *
	 * @return db type; MUST NOT be NULL
	 */
	DBType getDbType();

	/**
	 * Allows hosting application to suspend/resume cluster-tasks-service work (tasking and maintenance) as a reaction on runtime conditions
	 *
	 * @return false if cluster-tasks-service should be suspended
	 */
	default boolean isEnabled() {
		return true;
	}
}
