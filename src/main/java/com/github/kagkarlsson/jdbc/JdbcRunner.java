/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class JdbcRunner {
	private static final Logger LOG = LoggerFactory.getLogger(JdbcRunner.class);
	private final ConnectionSupplier connectionSupplier;

	public JdbcRunner(DataSource dataSource) {
		this(dataSource, false);
	}

	public JdbcRunner(DataSource dataSource, boolean commitWhenAutocommitDisabled) {
		this(new ConnectionSupplier() {
			@Override
			public Connection getConnection() throws SQLException {
				// detect if TransactionManager has an ongoing transaction
				// if so, use that, but should never commit here...
				return dataSource.getConnection();
			}

			@Override
			public boolean commitWhenAutocommitDisabled() {
				return commitWhenAutocommitDisabled;
			}
		});
	}

	public JdbcRunner(ConnectionSupplier connectionSupplier) {
		this.connectionSupplier = connectionSupplier;
	}

	public <T> T inTransaction(Function<JdbcRunner, T> doInTransaction) {
		return new TransactionManager(connectionSupplier).inTransaction(c -> {
			final JdbcRunner jdbc = new JdbcRunner(new ConnectionSupplier() {
				@Override
				public Connection getConnection() {
					return c;
				}

				@Override
				public boolean commitWhenAutocommitDisabled() {
					return false;
				}
			});
			return doInTransaction.apply(jdbc);
		});
	}

	public int execute(String query, PreparedStatementSetter setParameters) {
		return execute(query, setParameters, Statement::getUpdateCount);
	}

	public <T> List<T> query(String query, PreparedStatementSetter setParameters, RowMapper<T> rowMapper) {
		return execute(query, setParameters, (PreparedStatement p) -> mapResultSet(p, rowMapper));
	}

	public <T> T query(String query, PreparedStatementSetter setParameters, ResultSetMapper<T> resultSetMapper) {
		return execute(query, setParameters, (PreparedStatement p) -> mapResultSet(p, resultSetMapper));
	}

	private <T> T execute(String query, PreparedStatementSetter setParameters, AfterExecution<T> afterExecution) {
		return withConnection(c -> {

			PreparedStatement preparedStatement = null;
			try {

				try {
					preparedStatement = c.prepareStatement(query);
				} catch (SQLException e) {
					throw new SQLRuntimeException("Error when preparing statement.", e);
				}

				try {
					LOG.trace("Setting parameters of prepared statement.");
					setParameters.setParameters(preparedStatement);
				} catch (SQLException e) {
					throw new SQLRuntimeException(e);
				}

				try {
					LOG.trace("Executing prepared statement");
					preparedStatement.execute();
					return afterExecution.doAfterExecution(preparedStatement);
				} catch (SQLException e) {
					throw translateException(e);
				}

			} finally {
				nonThrowingClose(preparedStatement);
			}
		});
	}

	private void commitIfNecessary(Connection c) {
		try {
			if (connectionSupplier.commitWhenAutocommitDisabled() && !c.getAutoCommit()) {
				c.commit();
			}
		} catch (SQLException e) {
			throw new SQLRuntimeException("Failed to commit.", e);
		}
	}

	private void rollbackIfNecessary(Connection c) {
		try {
			if (connectionSupplier.commitWhenAutocommitDisabled() && !c.getAutoCommit()) {
				c.rollback();
			}
		} catch (SQLException e) {
			throw new SQLRuntimeException("Failed to rollback.", e);
		}
	}

	private SQLRuntimeException translateException(SQLException ex) {
		if (ex instanceof SQLIntegrityConstraintViolationException) {
			return new IntegrityConstraintViolation(ex);
		} else {
			return new SQLRuntimeException(ex);
		}
	}

	private <T> T withConnection(Function<Connection, T> doWithConnection) {
		Connection c;
		try {
			LOG.trace("Getting connection from datasource");
			c = connectionSupplier.getConnection();
		} catch (SQLException e) {
			throw new SQLRuntimeException("Unable to open connection", e);
		}

		try {
			final T result = doWithConnection.apply(c);
			commitIfNecessary(c);
			return result;
		} catch (RuntimeException | Error e) {
			rollbackIfNecessary(c);
			throw e;
		} finally {
			nonThrowingClose(c);
		}
	}

	private <T> List<T> mapResultSet(PreparedStatement executedPreparedStatement, RowMapper<T> rowMapper) {
		return withResultSet(
				executedPreparedStatement,
				(ResultSet rs) -> {
					List<T> results = new ArrayList<>();
					while (rs.next()) {
						results.add(rowMapper.map(rs));
					}
					return results;
				});
	}

	private <T> T mapResultSet(PreparedStatement executedPreparedStatement, ResultSetMapper<T> resultSetMapper) {
		return withResultSet(
				executedPreparedStatement,
				(ResultSet rs) -> resultSetMapper.map(rs)
		);
	}

	private <T> T withResultSet(PreparedStatement executedPreparedStatement, DoWithResultSet<T> doWithResultSet) {
		ResultSet rs = null;
		try {
			try {
				rs = executedPreparedStatement.getResultSet();
			} catch (SQLException e) {
				throw new SQLRuntimeException(e);
			}

			try {
				return doWithResultSet.withResultSet(rs);
			} catch (SQLException e) {
				throw new SQLRuntimeException(e);
			}

		} finally {
			nonThrowingClose(rs);
		}
	}


	private void nonThrowingClose(AutoCloseable toClose) {
		if (toClose == null) {
			return;
		}
		try {
			LOG.trace("Closing " + toClose.getClass().getSimpleName());
			toClose.close();
		} catch (Exception e) {
			LOG.warn("Exception on close of " + toClose.getClass().getSimpleName(), e);
		}
	}

	interface AfterExecution<T> {
		T doAfterExecution(PreparedStatement executedPreparedStatement) throws SQLException;
	}

	interface DoWithResultSet<T> {
		T withResultSet(ResultSet rs) throws SQLException;
	}
}
