package com.microfocus.octane.cluster.tasks.impl;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by gullery on 22/02/2017
 */

class BodyTablePreTruncateVerificationResult {
	private final List<Entry> entries = new LinkedList<>();

	void addEntry(Long bodyId, String body, Long metaId) {
		entries.add(new Entry(bodyId, body, metaId));
	}

	List<Entry> getEntries() {
		return entries;
	}

	static class Entry {
		final Long bodyId;
		final String body;
		final Long metaId;

		private Entry(Long bodyId, String body, Long metaId) {
			this.bodyId = bodyId;
			this.body = body;
			this.metaId = metaId;
		}

		@Override
		public String toString() {
			return "Entry: {" +
					" bodyId: " + bodyId +
					", body: " + body +
					", metaId: " + metaId +
					"}";
		}
	}
}
