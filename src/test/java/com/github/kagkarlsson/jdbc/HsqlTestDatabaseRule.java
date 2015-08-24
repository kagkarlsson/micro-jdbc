package com.github.kagkarlsson.jdbc;

import org.hsqldb.Database;
import org.hsqldb.DatabaseManager;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.rules.ExternalResource;

import javax.sql.DataSource;

public class HsqlTestDatabaseRule extends ExternalResource {

	private DataSource dataSource;

	@Override
	protected void before() throws Throwable {
		final JDBCDataSource ds = new JDBCDataSource();
		ds.setUrl("jdbc:hsqldb:mem:jdbcrunner");
		ds.setUser("sa");

		dataSource = ds;
	}

	@Override
	protected void after() {
		DatabaseManager.closeDatabases(Database.CLOSEMODE_IMMEDIATELY);
	}

	public DataSource getDataSource() {
		return dataSource;
	}
}
