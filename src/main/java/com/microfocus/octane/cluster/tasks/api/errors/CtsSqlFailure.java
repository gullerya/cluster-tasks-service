package com.microfocus.octane.cluster.tasks.api.errors;

import java.sql.SQLException;

public class CtsSqlFailure extends RuntimeException {
	public CtsSqlFailure(String message, SQLException sqle) {
		super(message, sqle);
	}
}
