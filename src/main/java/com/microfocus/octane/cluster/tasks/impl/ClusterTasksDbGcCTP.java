package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorScheduled;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksServiceConfigurerSPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ClusterTasksDbGcCTP extends ClusterTasksProcessorScheduled {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksDbGcCTP.class);

	private long totalGCRounds = 0;
	private long totalGCDuration = 0;
	private long totalFailures = 0;

	@Autowired
	private ClusterTasksServiceConfigurerSPI serviceConfigurer;
	@Autowired
	private ClusterTasksDbDataProvider dbDataProvider;

	private ClusterTasksDbGcCTP() {
		super(ClusterTasksDataProviderType.DB, ClusterTasksServiceConfigurerSPI.DEFAULT_GC_INTERVAL, 10 * 60 * 1000L);
	}

	@Override
	public void processTask(ClusterTask task) throws Exception {
		long gcStarted = System.currentTimeMillis();
		long gcDuration;
		try {
			dbDataProvider.handleGarbageAndStaled();
		} catch (Exception e) {
			totalFailures++;
			logger.error("failed to perform GC round; total failures: " + totalFailures, e);
		} finally {
			gcDuration = System.currentTimeMillis() - gcStarted;
			totalGCRounds++;
			totalGCDuration += gcDuration;
			if (totalGCRounds % 10 == 0) {
				logger.debug("GC executed in " + gcDuration + "ms; total GCs: " + totalGCRounds + "; average GC time: " + totalGCDuration / totalGCRounds + "ms");
			}

			if (serviceConfigurer != null) {
				Integer gcInterval = null;
				try {
					gcInterval = serviceConfigurer.getGCIntervalMillis();
				} catch (Exception e) {
					logger.error("failed to obtain GC interval from hosting application, falling back to default (" + ClusterTasksServiceConfigurerSPI.DEFAULT_GC_INTERVAL + ")", e);
				}
				gcInterval = gcInterval == null ? ClusterTasksServiceConfigurerSPI.DEFAULT_GC_INTERVAL : gcInterval;
				gcInterval = Math.max(gcInterval, ClusterTasksServiceConfigurerSPI.MINIMAL_GC_INTERVAL);
				setMinimalTasksTakeInterval(gcInterval);
			}
		}
	}
}
