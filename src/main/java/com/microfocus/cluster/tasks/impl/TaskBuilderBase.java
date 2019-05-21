/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.impl;

import com.microfocus.cluster.tasks.api.builders.TaskBuilders;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;

abstract public class TaskBuilderBase implements TaskBuilders.TaskBuilder {
	private final int MAX_APPLICATION_KEY_LENGTH = 64;

	protected volatile boolean locked;
	protected String uniquenessKey;
	protected String concurrencyKey;
	protected boolean concurrencyKeyUntouched;
	protected String applicationKey;
	private Long delayByMillis;
	private String body;

	protected TaskBuilderBase() {
	}

	public TaskBuilders.TaskBuilder setApplicationKey(String applicationKey) {
		if (locked) {
			throw new IllegalStateException("task builder MAY BE used only once");
		}
		if (applicationKey == null || applicationKey.isEmpty()) {
			throw new IllegalArgumentException("application key, if/when set, MUST NOT be NULL nor EMPTY");
		}
		if (applicationKey.length() > MAX_APPLICATION_KEY_LENGTH) {
			throw new IllegalArgumentException("application key MAY NOT exceed " + MAX_APPLICATION_KEY_LENGTH + " characters length");
		}
		this.applicationKey = applicationKey;
		return this;
	}

	public TaskBuilders.TaskBuilder setDelayByMillis(long delayByMillis) {
		if (locked) {
			throw new IllegalStateException("task builder MAY BE used only once");
		}
		if (delayByMillis < 0) {
			throw new IllegalArgumentException("delay MUST NOT be negative number");
		}
		this.delayByMillis = delayByMillis;
		return this;
	}

	public TaskBuilders.TaskBuilder setBody(String body) {
		if (locked) {
			throw new IllegalStateException("task builder MAY BE used only once");
		}
		if (body == null || body.isEmpty()) {
			throw new IllegalArgumentException("body, if/when set, MUST NOT be NULL nor EMPTY");
		}
		this.body = body;
		return this;
	}

	public ClusterTask build() {
		if (locked) throw new IllegalStateException("task builder MAY BE used only once");
		locked = true;
		ClusterTaskImpl result = new ClusterTaskImpl();
		result.uniquenessKey = uniquenessKey;
		result.concurrencyKey = concurrencyKey;
		result.concurrencyKeyUntouched = concurrencyKeyUntouched;
		result.delayByMillis = delayByMillis;
		result.body = body;
		return result;
	}
}
