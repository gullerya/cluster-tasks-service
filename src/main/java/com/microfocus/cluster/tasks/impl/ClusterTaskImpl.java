/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.impl;

import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskType;

/**
 * Created by gullery on 26/05/2016.
 * <p>
 * DTO bearing the task's information and used in consumer's oriented flows:
 * - when the tasks are submitted for enqueue, immutable ClusterTask interface exposed to the consumer having this implementation under the hood
 * - when the tasks are handed over to the processor, same as above is happening
 */

class ClusterTaskImpl implements ClusterTask {
	Long id;
	ClusterTaskType taskType = ClusterTaskType.REGULAR;
	String processorType;
	String uniquenessKey;
	String concurrencyKey;
	boolean concurrencyKeyUntouched;
	String applicationKey;
	Long orderingFactor;
	Long delayByMillis;
	String body;
	Long partitionIndex;

	ClusterTaskImpl() {
	}

	ClusterTaskImpl(ClusterTaskImpl origin) {
		id = origin.id;
		taskType = origin.taskType;
		processorType = origin.processorType;
		uniquenessKey = origin.uniquenessKey;
		concurrencyKey = origin.concurrencyKey;
		concurrencyKeyUntouched = origin.concurrencyKeyUntouched;
		applicationKey = origin.applicationKey;
		orderingFactor = origin.orderingFactor;
		delayByMillis = origin.delayByMillis;
		body = origin.body;
		partitionIndex = origin.partitionIndex;
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
	public String getBody() {
		return body;
	}

	@Override
	public String toString() {
		return "ClusterTaskImpl {" +
				"id: " + id +
				", taskType: " + taskType +
				", processorType: " + processorType +
				", uniquenessKey: " + uniquenessKey +
				", concurrencyKey: " + concurrencyKey +
				", applicationKey: " + applicationKey +
				", orderingFactor: " + orderingFactor +
				", delayByMillis: " + delayByMillis +
				", bodyLength: " + (body != null && !body.isEmpty() ? body.length() : "null") +
				", partitionIndex: " + partitionIndex + "}";
	}
}
