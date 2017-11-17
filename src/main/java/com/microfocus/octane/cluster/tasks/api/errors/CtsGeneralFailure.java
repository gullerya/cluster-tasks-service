package com.microfocus.octane.cluster.tasks.api.errors;

public class CtsGeneralFailure extends RuntimeException {
	public CtsGeneralFailure(String message, Exception e) {
		super(message, e);
	}
}
