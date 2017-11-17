package com.microfocus.octane.cluster.tasks.api.enums;

/**
 * Created by gullery on 08/06/2017
 */

public enum ClusterTaskStatus {
	PENDING(0),
	RUNNING(1),
	FINISHED(2);

	public final int value;

	ClusterTaskStatus(int value) {
		this.value = value;
	}
}
