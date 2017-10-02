package com.microfocus.octane.cluster.tasks;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.microfocus.octane.jdbc.api.JdbcService;
import com.microfocus.octane.jdbc.impl.config.JdbcConfigurationService;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.util.concurrent.CompletableFuture;

public class ClusterTasksServiceConfigurerForTestsSPI implements ClusterTasksServiceConfigurerSPI {
	private CompletableFuture<Boolean> configReadyLatch = new CompletableFuture<>();
	private DBType dbType;
	private DataSource dataSource;

	@Autowired
	private void processJdbcData(JdbcService jdbcService, JdbcConfigurationService configurationService) {
		switch (jdbcService.getDataSourceHolder().getDbType()) {
			case ORACLE:
				dbType = DBType.ORACLE;
				break;
			case MSSQL:
				dbType = DBType.MSSQL;
				break;
		}
		dataSource = jdbcService.getDataSourceHolder().getDataSource();
		configReadyLatch.complete(true);
	}

	@Override
	public CompletableFuture<Boolean> getConfigReadyLatch() {
		return configReadyLatch;
	}

	@Override
	public Integer getTasksPollIntervalMillis() {
		return null;
	}

	@Override
	public Integer getGCIntervalMillis() {
		return null;
	}

	@Override
	public DBType getDbType() {
		return dbType;
	}

	@Override
	public DataSource getDataSource() {
		return dataSource;
	}
}
