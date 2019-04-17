package com.microfocus.cluster.tasks.impl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

class CTSHashingUtils {
	private static final Map<String, String> hashes = new HashMap<>();

	private CTSHashingUtils() {
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
			hashes.put(input, Base64.getEncoder().encodeToString(bytes).substring(0, 6));
		}
		return hashes.get(input);
	}
}
