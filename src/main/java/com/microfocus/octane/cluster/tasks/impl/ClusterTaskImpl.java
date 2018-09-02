/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;

/**
 * Created by gullery on 26/05/2016.
 * <p>
 * DTO bearing the task's information and used in consumer's oriented flows:
 * - when the tasks are submitted for enqueue, immutable ClusterTask interface exposed to the consumer having this implementation under the hood
 * - when the tasks are handed over to the processor, same as above is happening
 */

class ClusterTaskImpl implements ClusterTask {
	Long id;
	String uniquenessKey;
	String concurrencyKey;
	Long orderingFactor;
	Long delayByMillis;
	Long maxTimeToRunMillis;
	String body;

	ClusterTaskImpl() {
	}

	@Override
	public Long getId() {
		return id;
	}

	@Override
	public String getUniquenessKey() {
		return uniquenessKey;
	}

	@Override
	public String getConcurrencyKey() {
		return concurrencyKey;
	}

	@Override
	public Long getOrderingFactor() {
		return orderingFactor;
	}

	@Override
	public Long getDelayByMillis() {
		return delayByMillis;
	}

	@Override
	public Long getMaxTimeToRunMillis() {
		return maxTimeToRunMillis;
	}

	@Override
	public String getBody() {
		return body;
	}

	static ClusterTask from(TaskInternal origin) {
		ClusterTaskImpl result = new ClusterTaskImpl();
		result.id = origin.id;
		result.uniquenessKey = origin.uniquenessKey;
		result.concurrencyKey = origin.concurrencyKey;
		result.orderingFactor = origin.orderingFactor;
		result.delayByMillis = origin.delayByMillis;
		result.maxTimeToRunMillis = origin.maxTimeToRunMillis;
		result.body = origin.body;
		return result;
	}

	@Override
	public String toString() {
		return "TaskToProcess {" +
				", id: " + id +
				", uniquenessKey: " + uniquenessKey +
				", concurrencyKey: " + concurrencyKey +
				", orderingFactor: " + orderingFactor +
				", delayByMillis: " + delayByMillis +
				", maxTimeToRunMillis: " + maxTimeToRunMillis +
				", bodyLength: " + (body != null && !body.isEmpty() ? body.length() : "null") +
				"}";
	}
}
