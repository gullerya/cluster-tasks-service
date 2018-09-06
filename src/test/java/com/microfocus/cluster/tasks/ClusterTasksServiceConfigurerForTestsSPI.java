package com.microfocus.cluster.tasks;

import com.microfocus.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static com.microfocus.cluster.tasks.api.ClusterTasksServiceConfigurerSPI.DBType.MSSQL;
import static com.microfocus.cluster.tasks.api.ClusterTasksServiceConfigurerSPI.DBType.ORACLE;
import static com.microfocus.cluster.tasks.api.ClusterTasksServiceConfigurerSPI.DBType.POSTGRESQL;

public class ClusterTasksServiceConfigurerForTestsSPI implements ClusterTasksServiceConfigurerSPI {
	private final CompletableFuture<Boolean> configReadyLatch = new CompletableFuture<>();
	private final DBType dbType;
	private final DataSource dataSource;

	private ClusterTasksServiceConfigurerForTestsSPI() throws IOException {
		Properties jdbcProperties = new Properties();
		String jdbcConfigFile;
		if ((jdbcConfigFile = System.getProperty("jdbc.config.location")) != null && !jdbcConfigFile.isEmpty()) {
			jdbcProperties.load(new FileInputStream(jdbcConfigFile));
		} else {
			jdbcProperties.load(this.getClass().getClassLoader().getResourceAsStream("db.properties"));
		}

		if (!jdbcProperties.containsKey("type")) {
			throw new IllegalStateException("DB type not specified");
		}

		String jdbcDriverClass;
		switch (jdbcProperties.getProperty("type")) {
			case "MSSQL":
				dbType = MSSQL;
				jdbcDriverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
				break;
			case "ORACLE":
				dbType = ORACLE;
				jdbcDriverClass = "oracle.jdbc.OracleDriver";
				break;
			case "POSTGRESQL":
				dbType = POSTGRESQL;
				jdbcDriverClass = "org.postgresql.Driver";
				break;
			default:
				throw new IllegalStateException("unsupported DB type specified: " + jdbcProperties.getProperty("type"));
		}

		HikariDataSource hikariDataSource = new HikariDataSource();
		hikariDataSource.setDriverClassName(jdbcDriverClass);
		hikariDataSource.setJdbcUrl(jdbcProperties.getProperty("url"));
		hikariDataSource.setUsername(jdbcProperties.getProperty("username"));
		hikariDataSource.setPassword(jdbcProperties.getProperty("password"));
		hikariDataSource.validate();

		dataSource = hikariDataSource;
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

	@Override
	public boolean isEnabled() {
		return true;
	}
}
