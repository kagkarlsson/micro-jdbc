package com.kagkarlsson.jdbc;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.Before;
import org.junit.Test;

public class JdbcRunnerIT {

	private JdbcRunner jdbcRunner;

	@Before
	public void setUp() {
		final JDBCDataSource ds = new JDBCDataSource();
		ds.setUrl("jdbc:hsqldb:mem:jdbcrunner");
		ds.setUser("sa");

		jdbcRunner = new JdbcRunner(ds);
	}

	@Test
	public void test() {
		jdbcRunner.execute("create table table1 ( hej INT);", preparedStatement -> {
		});
	}


}
