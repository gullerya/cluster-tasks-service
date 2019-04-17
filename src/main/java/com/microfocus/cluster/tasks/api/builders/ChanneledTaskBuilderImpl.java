/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.api.builders;

import com.microfocus.cluster.tasks.impl.TaskBuilderBase;

class ChanneledTaskBuilderImpl extends TaskBuilderBase implements TaskBuilders.ChanneledTaskBuilder {
	public TaskBuilders.TaskBuilder setConcurrencyKey(String concurrencyKey) throws IllegalStateException, IllegalArgumentException {
		if (locked) throw new IllegalStateException("task builder MAY BE used only once");
		if (concurrencyKey == null || concurrencyKey.isEmpty()) {
			throw new IllegalArgumentException("concurrency key MUST NOT be null nor empty");
		}
		if (concurrencyKey.length() > 34) {
			throw new IllegalArgumentException("concurrency key's length MUST BE less than or equal to 34 chars");
		}
		this.concurrencyKey = concurrencyKey;
		return this;
	}
}
