package com.microfocus.octane.cluster.tasks.api;

import com.microfocus.octane.cluster.tasks.impl.ClusterTaskInternal;
import com.microfocus.octane.cluster.tasks.impl.ClusterTaskType;

public final class ClusterTaskBuilders {

	RegularTaskBuilder getRegularTaskBuilder() {
		return new RegularTaskBuilder();
	}

	public final class RegularTaskBuilder {
		private boolean sealed = false;

		private Long id;
		private ClusterTaskType taskType;
		private String processorType;
		private String uniquenessKey;
		private String concurrencyKey;
		private Long orderingFactor;
		private Long delayByMillis;
		private Long maxTimeToRunMillis;
		private String body;

		private RegularTaskBuilder() {
		}

		public RegularTaskBuilder setId(Long id) {
			if (sealed) throw new IllegalStateException("this builder's result was already consumed, obtain new one");
			this.id = id;
			return this;
		}

		public RegularTaskBuilder setTaskType(ClusterTaskType taskType) {
			if (sealed) throw new IllegalStateException("this builder's result was already consumed, obtain new one");
			this.taskType = taskType;
			return this;
		}

		public RegularTaskBuilder setProcessorType(String processorType) {
			if (sealed) throw new IllegalStateException("this builder's result was already consumed, obtain new one");
			this.processorType = processorType;
			return this;
		}

		public RegularTaskBuilder setUniquenessKey(String uniquenessKey) {
			if (sealed) throw new IllegalStateException("this builder's result was already consumed, obtain new one");
			this.uniquenessKey = uniquenessKey;
			return this;
		}

		public RegularTaskBuilder setConcurrencyKey(String concurrencyKey) {
			if (sealed) throw new IllegalStateException("this builder's result was already consumed, obtain new one");
			this.concurrencyKey = concurrencyKey;
			return this;
		}

		public RegularTaskBuilder setOrderingFactor(Long orderingFactor) {
			if (sealed) throw new IllegalStateException("this builder's result was already consumed, obtain new one");
			this.orderingFactor = orderingFactor;
			return this;
		}

		public RegularTaskBuilder setDelayByMillis(Long delayByMillis) {
			if (sealed) throw new IllegalStateException("this builder's result was already consumed, obtain new one");
			this.delayByMillis = delayByMillis;
			return this;
		}

		public RegularTaskBuilder setMaxTimeToRunMillis(Long maxTimeToRunMillis) {
			if (sealed) throw new IllegalStateException("this builder's result was already consumed, obtain new one");
			this.maxTimeToRunMillis = maxTimeToRunMillis;
			return this;
		}

		public RegularTaskBuilder setBody(String body) {
			if (sealed) throw new IllegalStateException("this builder's result was already consumed, obtain new one");
			this.body = body;
			return this;
		}

		public ClusterTask build() {
			if (sealed) throw new IllegalStateException("this builder's result was already consumed, obtain new one");
			sealed = true;
			return new ClusterTask(id, taskType, processorType, uniquenessKey, concurrencyKey, orderingFactor, delayByMillis, maxTimeToRunMillis, body);
		}
	}
}
