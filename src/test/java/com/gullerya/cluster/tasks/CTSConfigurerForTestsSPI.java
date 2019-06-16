/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.gullerya.cluster.tasks;

import com.gullerya.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.DisposableBean;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class CTSConfigurerForTestsSPI implements ClusterTasksServiceConfigurerSPI, DisposableBean {
	private final CompletableFuture<Boolean> configReadyLatch = new CompletableFuture<>();
	private final DBType dbType;
	private final HikariDataSource dataSource;

	private CTSConfigurerForTestsSPI() throws IOException {
		Properties dbConfig = resolveConfigProperties();

		String jdbcDriverClass;
		switch (dbConfig.getProperty("type")) {
			case "MSSQL":
				dbType = DBType.MSSQL;
				jdbcDriverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
				break;
			case "ORACLE":
				dbType = DBType.ORACLE;
				jdbcDriverClass = "oracle.jdbc.OracleDriver";
				break;
			case "POSTGRESQL":
				dbType = DBType.POSTGRESQL;
				jdbcDriverClass = "org.postgresql.Driver";
				break;
			default:
				throw new IllegalStateException("unsupported DB type specified: " + dbConfig.getProperty("type"));
		}

		HikariDataSource hikariDataSource = new HikariDataSource();
		hikariDataSource.setDriverClassName(jdbcDriverClass);
		hikariDataSource.setJdbcUrl(dbConfig.getProperty("url"));
		hikariDataSource.setUsername(dbConfig.getProperty("username") != null ? dbConfig.getProperty("username") : "");
		hikariDataSource.setPassword(dbConfig.getProperty("password") != null ? dbConfig.getProperty("password") : "");
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

	private Properties resolveConfigProperties() throws IOException {
		Properties result = new Properties();
		String dbConfigLocation;
		if ((dbConfigLocation = System.getProperty("db.config.location")) != null && !dbConfigLocation.isEmpty()) {
			if ("environment".equals(dbConfigLocation)) {
				result.setProperty("type", System.getProperty("tests.db.type") != null ? System.getProperty("tests.db.type") : "");
				result.setProperty("url", System.getProperty("tests.db.url") != null ? System.getProperty("tests.db.url") : "");
				result.setProperty("username", System.getProperty("tests.db.username") != null ? System.getProperty("tests.db.username") : "");
				result.setProperty("password", System.getProperty("tests.db.password") != null ? System.getProperty("tests.db.password") : "");
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

	@Override
	public void destroy() {
		System.out.println("closing connections pool...");
		dataSource.close();
		System.out.println("connections pool closed");
	}
}
