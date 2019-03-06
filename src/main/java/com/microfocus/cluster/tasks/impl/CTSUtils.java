/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.microfocus.cluster.tasks.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

final class CTSUtils {
	private static final Logger logger = LoggerFactory.getLogger(CTSUtils.class);

	private CTSUtils() {
	}

	static boolean retry(int maxAttempts, Supplier<Boolean> supplier) {
		Boolean done = false;
		int attempts = 0;
		do {
			attempts++;
			try {
				done = supplier.get();
			} catch (Throwable t) {
				logger.error("failed to perform retryable action, attempt/s " + attempts + " out of max " + maxAttempts, t);
			}
		} while ((done == null || !done) && attempts < maxAttempts);

		if (done != null && done && attempts > 1) {
			logger.info("finally succeeded to perform retryable action (took " + attempts + " attempts)");
		}

		return done != null && done;
	}
}
