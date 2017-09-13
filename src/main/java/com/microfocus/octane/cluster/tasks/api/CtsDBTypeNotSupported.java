package com.microfocus.octane.cluster.tasks.api;

public class CtsDBTypeNotSupported extends RuntimeException {
	public CtsDBTypeNotSupported(String dbType) {
		super("DB type " + dbType + " is not supported");
	}
}
