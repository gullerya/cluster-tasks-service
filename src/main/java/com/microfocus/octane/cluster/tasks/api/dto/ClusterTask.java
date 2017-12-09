package com.microfocus.octane.cluster.tasks.api.dto;

/**
 * Created by gullery on 26/05/2016.
 * <p>
 * DTO bearing the task's information when
 * - builders used to create the tasks and enqueue them
 * - tasks are handed over to processor
 */

public interface ClusterTask {
	Long getId();

	String getUniquenessKey();

	String getConcurrencyKey();

	Long getOrderingFactor();

	Long getDelayByMillis();

	Long getMaxTimeToRunMillis();

	String getBody();
}
