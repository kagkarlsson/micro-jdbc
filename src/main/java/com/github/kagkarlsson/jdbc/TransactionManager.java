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

import java.sql.Connection;
import java.sql.SQLException;

public class TransactionManager {

	private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

	ThreadLocal<Connection> currentTransaction = new ThreadLocal<>();
	private final ConnectionSupplier dataSource;

	public TransactionManager(ConnectionSupplier dataSource) {
		this.dataSource = dataSource;
	}

	public <T> T inTransaction(DoInTransaction<T> doInTransaction) {
		if (currentTransaction.get() != null) {
			throw new SQLRuntimeException("Cannot start new transaction when there already is an ongoing transaction.");
		}

		try (Connection connection = dataSource.getConnection()) {
			boolean restoreAutocommit = false;

			if (connection.getAutoCommit()) {
				connection.setAutoCommit(false);
				restoreAutocommit = true;
			}

			try {
				final T result;
				try {
					currentTransaction.set(connection);
					result = doInTransaction.doInTransaction(connection);
				} catch (RuntimeException applicationException) {
					throw rollback(connection, applicationException);
				}

				commit(connection); // might throw
				return result;
			} finally {
				if (restoreAutocommit) {
					tryRestoreAutocommit(connection);
				}
			}

		} catch (SQLException openCloseAutocommitException) {
			throw new SQLRuntimeException(openCloseAutocommitException);
		} finally {
			currentTransaction.remove();
		}
	}

	private void tryRestoreAutocommit(Connection connection) {
		try {
			connection.setAutoCommit(true);
		} catch (SQLException e) {
			LOG.error("Failed to restore autocommit for Connection. Not throwing exception since the transaction has " +
					"already committed. Hopefully the connection-pool will mark the Connection as broken and not reuse.", e);
		}
	}


	private void commit(Connection connection) {
		try {
			connection.commit();
		} catch (SQLException commitException) {
			throw rollback(connection, new SQLRuntimeException(commitException));
		}
	}

	private RuntimeException rollback(Connection connection, RuntimeException originalException) {
		try {
			connection.rollback();
			return originalException;
		} catch (SQLException rollbackException) {
			LOG.error("Original application exception overridden by rollback-exception. Throwing rollback-exception. Original application exception: ", originalException);
			final SQLRuntimeException rollbackRuntimeException = new SQLRuntimeException(rollbackException);
			rollbackRuntimeException.addSuppressed(originalException);
			return rollbackRuntimeException;
		} catch (RuntimeException rollbackException) {
			LOG.error("Original application exception overridden by rollback-exception. Throwing rollback-exception. Original application exception: ", originalException);
			rollbackException.addSuppressed(originalException);
			return rollbackException;
		}
	}

}
