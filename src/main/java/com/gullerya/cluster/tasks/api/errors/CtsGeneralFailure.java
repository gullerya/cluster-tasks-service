package com.gullerya.cluster.tasks.api.errors;

public class CtsGeneralFailure extends RuntimeException {
	static final long serialVersionUID = -7124807193745766948L;

	public CtsGeneralFailure(String message, Throwable throwable) {
		super(message, throwable);
	}
}
