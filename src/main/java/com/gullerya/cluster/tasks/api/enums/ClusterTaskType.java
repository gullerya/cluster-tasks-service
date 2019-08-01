package com.gullerya.cluster.tasks.api.enums;

/**
 * Created by gullery on 14/08/2017
 */

public enum ClusterTaskType {
	REGULAR(0),
	SCHEDULED(1);

	public final long value;

	ClusterTaskType(long value) {
		this.value = value;
	}

	public static ClusterTaskType byValue(long numericValue) {
		for (ClusterTaskType taskType : values()) {
			if (taskType.value == numericValue) {
				return taskType;
			}
		}

		throw new IllegalArgumentException(numericValue + " is not a valid value");
	}
}
