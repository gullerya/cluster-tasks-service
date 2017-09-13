package com.microfocus.octane.cluster.tasks.api;

public class CtsGeneralFailure extends RuntimeException {
	public CtsGeneralFailure(String message, Exception e) {
		super(message, e);
	}
}
