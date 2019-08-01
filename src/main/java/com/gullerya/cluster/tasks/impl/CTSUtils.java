package com.gullerya.cluster.tasks.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.zip.CRC32;

final class CTSUtils {
	private static final Logger logger = LoggerFactory.getLogger(CTSUtils.class);
	private static final Map<String, String> hashes = new HashMap<>();

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

	/**
	 * this method is for internal usage ONLY
	 * creates weak 'hash', which is CRC32 checksum, encoded to Base64, of which only 6 significant characters are used
	 */
	static String get6CharsChecksum(String input) {
		if (!hashes.containsKey(input)) {
			CRC32 crc32 = new CRC32();
			crc32.update(input.getBytes(StandardCharsets.UTF_8));
			ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
			buffer.putLong(crc32.getValue());
			byte[] bytes = Arrays.copyOfRange(buffer.array(), 4, 8);
			String result = Base64.getEncoder().encodeToString(bytes).substring(0, 6);
			hashes.put(input, result);
			return result;
		}
		return hashes.get(input);
	}
}
