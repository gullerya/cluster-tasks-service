package com.microfocus.cluster.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

import static org.junit.Assert.fail;

/**
 * Created by gullery on 08/06/2017
 */

public class ClusterTasksTestsUtils {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksTestsUtils.class);

	private ClusterTasksTestsUtils() {
	}

	public static void waitSafely(long millisToSleep) {
		long started = System.currentTimeMillis();
		try {
			Thread.sleep(millisToSleep);
		} catch (InterruptedException ie) {
			System.out.println("interrupted while breathing");
			long leftToSleep = millisToSleep - (System.currentTimeMillis() - started);
			if (leftToSleep > 0) {
				System.out.println("left to sleep " + leftToSleep + ", falling asleep again...");
			}
		}
	}

	public static <V> V waitUntil(long maxTimeToWait, Supplier<V> verifier) {
		long started = System.currentTimeMillis();
		long pauseInterval = 50;
		V result;
		do {
			result = verifier.get();
			if (result == null) {
				waitSafely(pauseInterval);
			} else {
				logger.info("expectation fulfilled in " + (System.currentTimeMillis() - started) + "ms");
				break;
			}
		} while (System.currentTimeMillis() - started < maxTimeToWait);

		if (result == null) {
			fail("failed to fulfill expectation in " + maxTimeToWait + "ms");
			return null;
		} else {
			return result;
		}
	}
}
