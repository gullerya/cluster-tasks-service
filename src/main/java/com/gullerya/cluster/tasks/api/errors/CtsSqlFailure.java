package com.gullerya.cluster.tasks.api.errors;

import java.sql.SQLException;

public class CtsSqlFailure extends RuntimeException {
	static final long serialVersionUID = -7024897193745766949L;

	public CtsSqlFailure(String message, SQLException sqle) {
		super(message, sqle);
	}
}
