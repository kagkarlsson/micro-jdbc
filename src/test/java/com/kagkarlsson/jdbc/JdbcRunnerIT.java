package com.kagkarlsson.jdbc;

import org.hsqldb.Database;
import org.hsqldb.DatabaseManager;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.kagkarlsson.jdbc.PreparedStatementSetter.NOOP;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;

public class JdbcRunnerIT {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	private JdbcRunner jdbcRunner;

	@Before
	public void setUp() {
		final JDBCDataSource ds = new JDBCDataSource();
		ds.setUrl("jdbc:hsqldb:mem:jdbcrunner");
		ds.setUser("sa");

		jdbcRunner = new JdbcRunner(ds);
	}

	@After
	public void tearDown() {
		DatabaseManager.closeDatabases(Database.CLOSEMODE_IMMEDIATELY);
	}

	@Test
	public void test_basics() {
		jdbcRunner.execute("create table table1 ( column1 INT);", NOOP);
		final int inserted = jdbcRunner.execute("insert into table1(column1) values (?)", ps -> ps.setInt(1, 1));
		assertThat(inserted, is(1));

		final List<Integer> rowMapped = jdbcRunner.query("select * from table1", NOOP, new TableRowMapper());
		assertThat(rowMapped, hasSize(1));
		assertThat(rowMapped.get(0), is(1));

		assertThat(jdbcRunner.query("select * from table1", NOOP, Mappers.SINGLE_INT), is(1));

		final int updated = jdbcRunner.execute("update table1 set column1 = ? where column1 = ?",
				ps -> {
					ps.setInt(1, 5);
					ps.setInt(2, 1);
				});
		assertThat(updated, is(1));
	}

	@Test
	public void test_map_multiple_rows() {
		jdbcRunner.execute("create table table1 ( column1 INT);", NOOP);
		jdbcRunner.execute("insert into table1(column1) values (1)", NOOP);
		jdbcRunner.execute("insert into table1(column1) values (2)", NOOP);

		final List<Integer> rowMapped = jdbcRunner.query("select * from table1", NOOP, new TableRowMapper());
		assertThat(rowMapped, hasSize(2));
		assertThat(rowMapped.get(0), is(1));
		assertThat(rowMapped.get(1), is(2));

		final List<Integer> resultSetMapped = jdbcRunner.query("select * from table1", NOOP, new TableRowMapper());
		assertThat(resultSetMapped, hasSize(2));
		assertThat(resultSetMapped.get(0), is(1));
		assertThat(resultSetMapped.get(1), is(2));
	}

	@Test
	public void should_map_constraint_violations_to_custom_exception_for_primary_key_constraint() {
		expectedException.expect(IntegrityConstraintViolation.class);

		jdbcRunner.execute("create table table1 ( column1 INT PRIMARY KEY);", NOOP);
		jdbcRunner.execute("insert into table1(column1) values (1)", NOOP);
		jdbcRunner.execute("insert into table1(column1) values (1)", NOOP);
	}

	@Test
	public void should_map_constraint_violations_to_custom_exception_for_unique_constraint_() {
		expectedException.expect(IntegrityConstraintViolation.class);

		jdbcRunner.execute("create table table1 ( column1 INT);", NOOP);
		jdbcRunner.execute("alter table table1 add constraint col1_uidx unique (column1);", NOOP);
		jdbcRunner.execute("insert into table1(column1) values (1)", NOOP);
		jdbcRunner.execute("insert into table1(column1) values (1)", NOOP);
	}

	private static class TableRowMapper implements RowMapper<Integer> {
		@Override
		public Integer map(ResultSet rs) throws SQLException {
			return rs.getInt("column1");
		}
	}

	private static class ResultSetMapper implements com.kagkarlsson.jdbc.ResultSetMapper<List<Integer>> {

		@Override
		public List<Integer> map(ResultSet rs) throws SQLException {
			List<Integer> results = new ArrayList<>();
			while (rs.next()) {
				results.add(rs.getInt("column1"));
			}
			return results;
		}
	}


}
