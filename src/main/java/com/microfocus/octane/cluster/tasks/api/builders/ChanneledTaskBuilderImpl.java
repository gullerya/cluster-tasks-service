package com.microfocus.octane.cluster.tasks.api.builders;

import com.microfocus.octane.cluster.tasks.impl.TaskBuilderBase;

class ChanneledTaskBuilderImpl extends TaskBuilderBase implements ChanneledTaskBuilder {
	public TaskBuilder setConcurrencyKey(String concurrencyKey) throws IllegalStateException, IllegalArgumentException {
		if (locked) throw new IllegalStateException("task builder MAY BE used only once");
		if (concurrencyKey == null || concurrencyKey.isEmpty())
			throw new IllegalArgumentException("concurrency key MUST NOT be null nor empty");
		if (concurrencyKey.length() > 40)
			throw new IllegalArgumentException("concurrency key's length MUST BE less than or equal to 40 chars");
		this.concurrencyKey = concurrencyKey;
		return this;
	}
}
