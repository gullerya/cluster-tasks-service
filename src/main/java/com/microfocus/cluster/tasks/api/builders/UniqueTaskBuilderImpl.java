/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.api.builders;

import com.microfocus.cluster.tasks.impl.TaskBuilderBase;

class UniqueTaskBuilderImpl extends TaskBuilderBase implements TaskBuilders.UniqueTaskBuilder {
	public TaskBuilders.TaskBuilder setUniquenessKey(String uniquenessKey) throws IllegalStateException, IllegalArgumentException {
		if (locked) throw new IllegalStateException("task builder MAY BE used only once");
		if (uniquenessKey == null || uniquenessKey.isEmpty())
			throw new IllegalArgumentException("uniqueness key MUST NOT be null nor empty");
		if (uniquenessKey.length() > 40)
			throw new IllegalArgumentException("uniqueness key's length MUST BE less than or equal to 40 chars");
		this.uniquenessKey = uniquenessKey;
		return this;
	}
}
