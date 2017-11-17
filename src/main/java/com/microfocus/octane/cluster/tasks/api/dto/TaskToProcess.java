package com.microfocus.octane.cluster.tasks.api.dto;

/**
 * Created by gullery on 26/05/2016.
 * <p>
 * DTO bearing the task's information when handed over to processor
 */

public interface TaskToProcess {
	Long getId();

	String getUniquenessKey();

	String getConcurrencyKey();

	Long getOrderingFactor();

	Long getDelayByMillis();

	Long getMaxTimeToRunMillis();

	String getBody();
}
