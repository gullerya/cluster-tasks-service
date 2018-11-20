/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.api;

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
	 * Allows hosting application to suspend/resume cluster-tasks-service work (tasking and maintenance) as a reaction on runtime conditions
	 *
	 * @return false if cluster-tasks-service should be suspended
	 */
	boolean isEnabled();
}
