/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.gullerya.cluster.tasks.impl;

import com.gullerya.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

class ClusterTasksServiceSchemaManager {
	private final Logger logger = LoggerFactory.getLogger(ClusterTasksServiceSchemaManager.class);

	private static final String CTS_SCHEMA_HISTORY_TABLE_NAME = "CTS_SCHEMA_HISTORY";
	private static final String SQL_MIGRATION_PREFIX = "v";

	void executeSchemaMaintenance(ClusterTasksServiceConfigurerSPI.DBType dbType, DataSource dataSource) {
		Flyway flyway = new Flyway();
		try {
			flyway.setDataSource(dataSource);
			flyway.setTable(CTS_SCHEMA_HISTORY_TABLE_NAME);
			flyway.setSqlMigrationPrefix(SQL_MIGRATION_PREFIX);
			flyway.setBaselineOnMigrate(true);
			flyway.setValidateOnMigrate(true);
			flyway.setCleanDisabled(true);
			flyway.setLocations(getSQLsLocation(dbType));
			flyway.migrate();
		} catch (Exception e) {
			logger.error("DB maintenance failed, attempting to repair...", e);
			flyway.repair();
			logger.info("DB repair after migration failure has SUCCEED");
		}
	}

	private String getSQLsLocation(ClusterTasksServiceConfigurerSPI.DBType dbType) {
		String result;
		switch (dbType) {
			case ORACLE:
				result = "classpath:cts/schema/oracle";
				break;
			case MSSQL:
				result = "classpath:cts/schema/sqlserver";
				break;
			case POSTGRESQL:
				result = "classpath:cts/schema/postgresql";
				break;
			default:
				throw new IllegalArgumentException(dbType + " is not supported");
		}
		return result;
	}
}
