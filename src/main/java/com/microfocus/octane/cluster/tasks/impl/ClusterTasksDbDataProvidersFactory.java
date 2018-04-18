package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gullery on 08/05/2016.
 * <p>
 * Cluster tasks DB data provider's factory - resolving the relevant bean of the DbDataProvider based on the DataSource configuration (DbType)
 */

@Configuration
class ClusterTasksDbDataProvidersFactory {
	private final Map<ClusterTasksServiceConfigurerSPI.DBType, ClusterTasksDbDataProvider> dbDataProvidersMap;

	@Autowired
	private ClusterTasksServiceConfigurerSPI serviceConfigurer;

	ClusterTasksDbDataProvidersFactory() {
		this.dbDataProvidersMap = new HashMap<>();
		this.dbDataProvidersMap.put(ClusterTasksServiceConfigurerSPI.DBType.MSSQL, new MsSqlDbDataProvider());
		this.dbDataProvidersMap.put(ClusterTasksServiceConfigurerSPI.DBType.ORACLE, new OracleDbDataProvider());
		this.dbDataProvidersMap.put(ClusterTasksServiceConfigurerSPI.DBType.POSTGRESQL, new PostgreSqlDbDataProvider());
	}

	@Bean
	ClusterTasksDbDataProvider getDbDataProvider() {
		if (dbDataProvidersMap.containsKey(serviceConfigurer.getDbType())) {
			return dbDataProvidersMap.get(serviceConfigurer.getDbType());
		} else {
			throw new IllegalStateException("DB type '" + serviceConfigurer.getDbType() + "' has no registered data provider");
		}
	}
}
