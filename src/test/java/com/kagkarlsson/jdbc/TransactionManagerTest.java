package com.kagkarlsson.jdbc;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TransactionManagerTest {

	@Mock
	private DataSource dataSource;
	@Mock
	private Connection connection;
	private TransactionManager tm;

	@Before
	public void setUp() throws SQLException {
		when(dataSource.getConnection()).thenReturn(connection);
		when(connection.getAutoCommit()).thenReturn(false);

		tm = new TransactionManager(dataSource);
	}

	@Test
	public void should_commit_if_no_exceptions() throws SQLException {
		tm.inTransaction(() -> null);
		verify(connection).getAutoCommit();
		verify(connection).commit();
		verify(connection).close();
		verifyNoMoreInteractions(connection);

		assertThat(tm.currentTransaction.get(), nullValue());
	}

	@Test
	public void should_rollback_if_exception() throws SQLException {
		try {
			tm.inTransaction(() -> {throw new SQLRuntimeException();});
			fail("Should have thrown exception");
		} catch (SQLRuntimeException e) {
		}

		verify(connection).getAutoCommit();
		verify(connection).rollback();
		verify(connection).close();
		verifyNoMoreInteractions(connection);

		assertThat(tm.currentTransaction.get(), nullValue());
	}

	@Test
	public void should_rollback_if_sql_exception_on_commit() throws SQLException {
		doThrow(new SQLException()).when(connection).commit();
		try {
			tm.inTransaction(() -> null);
		} catch (SQLRuntimeException e) {
		}

		verify(connection).getAutoCommit();
		verify(connection).commit();
		verify(connection).rollback();
		verify(connection).close();
		verifyNoMoreInteractions(connection);

		assertThat(tm.currentTransaction.get(), nullValue());
	}

	@Test
	public void should_close_on_exception_on_commit_and_rollback() throws SQLException {
		doThrow(new SQLException()).when(connection).commit();
		doThrow(new SQLException()).when(connection).rollback();
		try {
			tm.inTransaction(() -> null);
		} catch (SQLRuntimeException e) {
		}

		verify(connection).getAutoCommit();
		verify(connection).commit();
		verify(connection).rollback();
		verify(connection).close();
		verifyNoMoreInteractions(connection);

		assertThat(tm.currentTransaction.get(), nullValue());
	}

	@Test
	public void should_restore_autocommit_if_enabled_on_connection_open() throws SQLException {
		when(connection.getAutoCommit()).thenReturn(true);
		tm.inTransaction(() -> null);

		verify(connection).getAutoCommit();
		verify(connection).setAutoCommit(true);
		verify(connection).commit();
		verify(connection).setAutoCommit(false);
		verify(connection).close();
		verifyNoMoreInteractions(connection);

		assertThat(tm.currentTransaction.get(), nullValue());
	}

}
