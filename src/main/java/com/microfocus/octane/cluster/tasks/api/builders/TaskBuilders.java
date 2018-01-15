package com.microfocus.octane.cluster.tasks.api.builders;

public class TaskBuilders {

	private TaskBuilders() {
	}

	public static TaskBuilder simpleTask() {
		return new SimpleTaskBuilder();
	}

	public static ChanneledTaskBuilder channeledTask() {
		return new ChanneledTaskBuilderImpl();
	}

	public static UniqueTaskBuilder uniqueTask() {
		return new UniqueTaskBuilderImpl();
	}
}
