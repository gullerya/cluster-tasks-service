package com.microfocus.octane.cluster.tasks.api.builders;

import com.microfocus.octane.cluster.tasks.impl.TaskBuilderBase;

class UniqueTaskBuilderImpl extends TaskBuilderBase implements UniqueTaskBuilder {
	public TaskBuilder setUniquenessKey(String uniquenessKey) throws IllegalStateException, IllegalArgumentException {
		if (locked) throw new IllegalStateException("task builder MAY BE used only once");
		if (uniquenessKey == null || uniquenessKey.isEmpty())
			throw new IllegalArgumentException("uniqueness key MUST NOT be null nor empty");
		if (uniquenessKey.length() > 40)
			throw new IllegalArgumentException("uniqueness key's length MUST BE less than or equal to 40 chars");
		this.uniquenessKey = uniquenessKey;
		return this;
	}
}
