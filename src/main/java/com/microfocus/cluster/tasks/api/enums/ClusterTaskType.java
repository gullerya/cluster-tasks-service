/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.api.enums;

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
