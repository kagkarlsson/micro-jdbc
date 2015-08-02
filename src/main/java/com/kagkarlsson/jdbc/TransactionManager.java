package com.kagkarlsson.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class TransactionManager {

	private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

	private final DataSource dataSource;

	public TransactionManager(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public <T> T inTransaction(DoInTransaction<T> doInTransaction) {
		//TODO check if already in transaction
		try (Connection connection = dataSource.getConnection()) {

			connection.setAutoCommit(false);

			final T result;
			try {
				result = doInTransaction.doInTransaction();
			} catch (RuntimeException applicationException) {
				rollback(connection, applicationException);
				throw applicationException;
			}

			commit(connection);
			return result;

		} catch (SQLException openCloseException) {
			throw new RuntimeException(openCloseException);
		}
	}

	private void commit(Connection connection) {
		try {
			connection.commit();
		} catch (SQLException commitException) {
			rollback(connection, commitException);
		}
	}

	private void rollback(Connection connection, Throwable originalException) {
		try {
			connection.rollback();
		} catch (SQLException rollbackException) {
			LOG.error("Original application exception overridden by rollback-exception. Throwing rollback-exception. Original application exception: ", originalException);
			throw new RuntimeException(rollbackException);
		} catch (RuntimeException rollbackException) {
			LOG.error("Original application exception overridden by rollback-exception. Throwing rollback-exception. Original application exception: ", originalException);
			throw rollbackException;
		}
	}

	public interface DoInTransaction<T> {
		T doInTransaction();
	}
}
