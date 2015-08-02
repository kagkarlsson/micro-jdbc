package com.kagkarlsson.jdbc;

import java.sql.SQLException;

public class IntegrityConstraintViolation extends RuntimeException {
	public IntegrityConstraintViolation(SQLException ex) {
		super(ex);
	}
}
