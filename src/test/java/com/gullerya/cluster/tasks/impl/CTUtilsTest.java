/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.gullerya.cluster.tasks.impl;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gullery on 02/06/2016.
 * <p>
 * Main collection of integration tests for Cluster Tasks Service's Utils
 */

public class CTUtilsTest {

	@Test
	public void testASimpleRetry() {
		boolean result = CTSUtils.retry(3, () -> true);
		Assert.assertTrue(result);
	}

	@Test
	public void testBSimpleRetry3TimesWithFalse() {
		AtomicInteger outerCount = new AtomicInteger();
		boolean result = CTSUtils.retry(3, () -> outerCount.incrementAndGet() == 3);
		Assert.assertTrue(result);
		Assert.assertEquals(3, outerCount.get());
	}

	@Test
	public void testCSimpleRetry2TimesWithNull() {
		AtomicInteger outerCount = new AtomicInteger();
		boolean result = CTSUtils.retry(3, () -> outerCount.incrementAndGet() == 2 ? true : null);
		Assert.assertTrue(result);
		Assert.assertEquals(2, outerCount.get());
	}

	@Test
	public void testDFailRetry3TimesWithFalse() {
		AtomicInteger outerCount = new AtomicInteger();
		boolean result = CTSUtils.retry(2, () -> outerCount.incrementAndGet() == 3);
		Assert.assertFalse(result);
		Assert.assertEquals(2, outerCount.get());
	}

	@Test
	public void testEFailRetry3TimesWithNull() {
		AtomicInteger outerCount = new AtomicInteger();
		boolean result = CTSUtils.retry(2, () -> outerCount.incrementAndGet() == 3 ? true : null);
		Assert.assertFalse(result);
		Assert.assertEquals(2, outerCount.get());
	}

	@Test
	public void testFFailRetryOnceWithException() {
		AtomicInteger outerCount = new AtomicInteger();
		boolean result = CTSUtils.retry(2, () -> {
			outerCount.incrementAndGet();
			throw new IllegalStateException("to fail");
		});
		Assert.assertFalse(result);
		Assert.assertEquals(2, outerCount.get());
	}

	@Test
	public void testGPassRetryFewTimesWithException() {
		AtomicInteger outerCount = new AtomicInteger();
		boolean result = CTSUtils.retry(4, () -> {
			if (outerCount.incrementAndGet() < 3) {
				throw new IllegalStateException("to fail");
			} else {
				return true;
			}
		});
		Assert.assertTrue(result);
		Assert.assertEquals(3, outerCount.get());
	}
}