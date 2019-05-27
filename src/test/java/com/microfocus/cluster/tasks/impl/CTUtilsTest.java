package com.microfocus.cluster.tasks.impl;

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
	public void testA_simple_retry() {
		boolean result = CTSUtils.retry(3, () -> true);
		Assert.assertTrue(result);
	}

	@Test
	public void testB_simple_retry_3_times_with_false() {
		AtomicInteger outerCount = new AtomicInteger();
		boolean result = CTSUtils.retry(3, () -> outerCount.incrementAndGet() == 3);
		Assert.assertTrue(result);
		Assert.assertEquals(3, outerCount.get());
	}

	@Test
	public void testC_simple_retry_2_times_with_null() {
		AtomicInteger outerCount = new AtomicInteger();
		boolean result = CTSUtils.retry(3, () -> outerCount.incrementAndGet() == 2 ? true : null);
		Assert.assertTrue(result);
		Assert.assertEquals(2, outerCount.get());
	}

	@Test
	public void testD_fail_retry_3_times_with_false() {
		AtomicInteger outerCount = new AtomicInteger();
		boolean result = CTSUtils.retry(2, () -> outerCount.incrementAndGet() == 3);
		Assert.assertFalse(result);
		Assert.assertEquals(2, outerCount.get());
	}

	@Test
	public void testE_fail_retry_3_times_with_null() {
		AtomicInteger outerCount = new AtomicInteger();
		boolean result = CTSUtils.retry(2, () -> outerCount.incrementAndGet() == 3 ? true : null);
		Assert.assertFalse(result);
		Assert.assertEquals(2, outerCount.get());
	}

	@Test
	public void testF_fail_retry_once_with_exception() {
		AtomicInteger outerCount = new AtomicInteger();
		boolean result = CTSUtils.retry(2, () -> {
			outerCount.incrementAndGet();
			throw new RuntimeException("to fail");
		});
		Assert.assertFalse(result);
		Assert.assertEquals(2, outerCount.get());
	}

	@Test
	public void testG_pass_retry_few_times_with_exception() {
		AtomicInteger outerCount = new AtomicInteger();
		boolean result = CTSUtils.retry(4, () -> {
			if (outerCount.incrementAndGet() < 3) {
				throw new RuntimeException("to fail");
			} else {
				return true;
			}
		});
		Assert.assertTrue(result);
		Assert.assertEquals(3, outerCount.get());
	}
}
