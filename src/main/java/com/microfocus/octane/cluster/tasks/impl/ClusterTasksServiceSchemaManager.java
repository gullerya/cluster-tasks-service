package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import org.flywaydb.core.Flyway;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;

@Service
class ClusterTasksServiceSchemaManager {
	private static final String CTS_SCHEMA_HISTORY_TABLE_NAME = "CTS_SCHEMA_HISTORY";
	private static final String SQL_MIGRATION_PREFIX = "v";

	void executeSchemaMaintenance(ClusterTasksServiceConfigurerSPI.DBType dbType, DataSource dataSource) {
		if (dbType == null) {
			throw new IllegalArgumentException("DB type MUST NOT be null");
		}
		if (dataSource == null) {
			throw new IllegalArgumentException("DataSource MUST NOT be null");
		}

		Flyway flyway = new Flyway();
		flyway.setDataSource(dataSource);
		flyway.setTable(CTS_SCHEMA_HISTORY_TABLE_NAME);
		flyway.setSqlMigrationPrefix(SQL_MIGRATION_PREFIX);
		flyway.setBaselineOnMigrate(true);
		flyway.setLocations(getSQLsLocation(dbType));
		flyway.migrate();
	}

	private String getSQLsLocation(ClusterTasksServiceConfigurerSPI.DBType dbType) {
		String result = null;
		switch (dbType) {
			case ORACLE:
				result = "classpath:cts/schema/oracle";
				break;
			case MSSQL:
				result = "classpath:cts/schema/sqlserver";
				break;
		}
		return result;
	}
}
