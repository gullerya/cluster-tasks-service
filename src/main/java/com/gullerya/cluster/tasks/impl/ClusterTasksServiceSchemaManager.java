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
		Flyway flyway = null;
		try {
			flyway = Flyway.configure()
					.dataSource(dataSource)
					.table(CTS_SCHEMA_HISTORY_TABLE_NAME)
					.sqlMigrationPrefix(SQL_MIGRATION_PREFIX)
					.baselineOnMigrate(true)
					.validateOnMigrate(true)
					.cleanDisabled(true)
					.locations(getSQLsLocation(dbType))
					.load();
			flyway.migrate();
		} catch (Exception e) {
			logger.error("DB maintenance failed, attempting to repair...", e);
			if (flyway != null) {
				flyway.repair();
			}
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
