package com.microfocus.octane.cluster.tasks;

/**
 * Created by gullery on 08/06/2017
 */

public class ClusterTasksITUtils {

	private ClusterTasksITUtils() {
	}

	public static void sleepSafely(long millisToSleep) {
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
}
