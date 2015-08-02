package com.kagkarlsson.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class JdbcRunner {
	private static final Logger LOG = LoggerFactory.getLogger(JdbcRunner.class);
	private final DataSource dataSource;

	public JdbcRunner(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public int execute(String query, PreparedStatementSetter setParameters) {
		return execute(query, setParameters, (PreparedStatement p) -> p.getUpdateCount());
	}

	public <T> List<T> query(String query, PreparedStatementSetter setParameters, RowMapper<T> rowMapper) {
		return execute(query, setParameters, (PreparedStatement p) -> mapResultSet(p, rowMapper));
	}

	public <T> T query(String query, PreparedStatementSetter setParameters, ResultSetMapper<T> resultSetMapper) {
		return execute(query, setParameters, (PreparedStatement p) -> mapResultSet(p, resultSetMapper));
	}

	private <T> T execute(String query, PreparedStatementSetter setParameters, AfterExecution<T> afterExecution) {
		return withConnection(c -> {

			List<AutoCloseable> closables = new ArrayList<>();

			try {
				PreparedStatement preparedStatement;
				try {
					preparedStatement = c.prepareStatement(query);
				} catch (SQLException e) {
					throw new RuntimeException("Error when preparing statement.", e);
				}
				closables.add(preparedStatement);

				try {
					setParameters.setParameters(preparedStatement);
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
				try {
					preparedStatement.execute();
					return afterExecution.doAfterExecution(preparedStatement);
				} catch (SQLException e) {
					throw translateException(e);
				}

			} finally {
				silentClose(closables);
			}
		});
	}

	private RuntimeException translateException(SQLException ex) {
		if (ex instanceof SQLIntegrityConstraintViolationException) {
			return new IntegrityConstraintViolation(ex);
		} else {
			return new RuntimeException(ex);
		}
	}

	private void silentClose(List<AutoCloseable> closeable) {
		for (int i = closeable.size() - 1; i >= 0; i--) {
			silentClose(closeable.get(i));
		}
	}

	private <T> T withConnection(Function<Connection, T> doWithConnection) {
		Connection c;
		try {
			c = dataSource.getConnection();
		} catch (SQLException e) {
			throw new RuntimeException("Unable to open connection", e);
		}

		try {
			return doWithConnection.apply(c);
		} finally {
			silentClose(c);
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
				throw new RuntimeException(e);
			}

			try {
				return doWithResultSet.withResultSet(rs);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}

		} finally {
			silentClose(rs);
		}
	}


	private void silentClose(AutoCloseable toClose) {
		if (toClose == null) {
			return;
		}
		try {
			LOG.trace("Closing " + toClose.getClass().getSimpleName());
			toClose.close();
		} catch (Exception e) {
			LOG.debug("Exception on close of " + toClose.getClass().getSimpleName(), e);
		}
	}

	interface AfterExecution<T> {
		T doAfterExecution(PreparedStatement executedPreparedStatement) throws SQLException;
	}

	interface DoWithResultSet<T> {
		T withResultSet(ResultSet rs) throws SQLException;
	}
}
