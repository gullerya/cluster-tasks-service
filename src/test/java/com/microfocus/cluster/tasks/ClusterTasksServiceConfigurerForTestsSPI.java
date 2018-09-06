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
		Properties dbConfig = resolveConfigProperties();

		String jdbcDriverClass;
		switch (dbConfig.getProperty("type")) {
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
				throw new IllegalStateException("unsupported DB type specified: " + dbConfig.getProperty("type"));
		}

		HikariDataSource hikariDataSource = new HikariDataSource();
		hikariDataSource.setDriverClassName(jdbcDriverClass);
		hikariDataSource.setJdbcUrl(dbConfig.getProperty("url"));
		hikariDataSource.setUsername(dbConfig.getProperty("username"));
		hikariDataSource.setPassword(dbConfig.getProperty("password"));
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

	private Properties resolveConfigProperties() throws IOException {
		Properties result = new Properties();
		String dbConfigLocation;
		if ((dbConfigLocation = System.getProperty("db.config.location")) != null && !dbConfigLocation.isEmpty()) {
			if ("environment".equals(dbConfigLocation)) {
				result.setProperty("type", System.getProperty("db.type"));
				result.setProperty("url", System.getProperty("db.url"));
				result.setProperty("username", System.getProperty("db.username"));
				result.setProperty("password", System.getProperty("db.password"));
			} else {
				result.load(new FileInputStream(dbConfigLocation));
			}
		} else {
			result.load(this.getClass().getClassLoader().getResourceAsStream("db.properties"));
		}

		//  basic validation
		if (!result.containsKey("type") || result.getProperty("type").isEmpty()) {
			throw new IllegalStateException("DB type invalid: [" + result.getProperty("type") + "]");
		}
		if (!result.containsKey("url") || result.getProperty("url").isEmpty()) {
			throw new IllegalStateException("DB url invalid: [" + result.getProperty("url") + "]");
		}

		return result;
	}
}
