package com.microfocus.octane.cluster.tasks;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.microfocus.octane.jdbc.api.JdbcService;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.util.concurrent.CompletableFuture;

public class ClusterTasksServiceConfigurerForTestsSPI implements ClusterTasksServiceConfigurerSPI {
	private CompletableFuture<Boolean> configReadyLatch = new CompletableFuture<>();
	private DBType dbType;
	private DataSource dataSource;

	@Autowired
	private void processJdbcData(JdbcService jdbcService) {
		switch (jdbcService.getDataSourceConfiguration().getDbType()) {
			case MSSQL:
				dbType = DBType.MSSQL;
				break;
			case ORACLE:
				dbType = DBType.ORACLE;
				break;
			case POSTGRESQL:
				dbType = DBType.POSTGRESQL;
				break;
		}
		dataSource = jdbcService.getDataSourceConfiguration().getDataSource();
		configReadyLatch.complete(true);
	}

	@Override
	public CompletableFuture<Boolean> getConfigReadyLatch() {
		return configReadyLatch;
	}

	@Override
	public DataSource getDataSource() {
		return dataSource;
	}

	@Override
	public DataSource getAdministrativeDataSource() {
		return dataSource;
	}

	@Override
	public DBType getDbType() {
		return dbType;
	}

	@Override
	public Integer getTasksPollIntervalMillis() {
		return null;
	}

	@Override
	public Integer getMaintenanceIntervalMillis() {
		return null;
	}
}
