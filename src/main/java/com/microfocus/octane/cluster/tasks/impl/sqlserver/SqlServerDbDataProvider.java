package com.microfocus.octane.cluster.tasks.impl.sqlserver;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI.DBType;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskStatus;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskType;
import com.microfocus.octane.cluster.tasks.api.errors.CtsDBTypeNotSupported;
import com.microfocus.octane.cluster.tasks.api.errors.CtsSqlFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by gullery on 12/04/2018.
 */

final class SqlServerDbDataProvider {
	private static final Logger logger = LoggerFactory.getLogger(SqlServerDbDataProvider.class);

}
