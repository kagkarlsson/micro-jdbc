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
import java.sql.Connection;
import java.sql.SQLException;

public class TransactionManager {

	private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

	ThreadLocal<Connection> currentTransaction = new ThreadLocal<>();
	private final DataSource dataSource;

	public TransactionManager(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public <T> T inTransaction(DoInTransaction<T> doInTransaction) {
		if (currentTransaction.get() != null) {
			throw new SQLRuntimeException("Cannot start new transaction when there already is an ongoing transaction.");
		}

		boolean restoreAutocommit = false;
		try (Connection connection = dataSource.getConnection()) {

			if (connection.getAutoCommit()) {
				connection.setAutoCommit(false);
				restoreAutocommit = true;
			}

			final T result;
			try {
				currentTransaction.set(connection);
				result = doInTransaction.doInTransaction();
			} catch (SQLRuntimeException applicationException) {
				rollback(connection, applicationException);
				throw applicationException;
			}

			commit(connection);
			if (restoreAutocommit) {
				restoreAutocommit(connection);
			}
			return result;

		} catch (SQLException openCloseException) {
			throw new SQLRuntimeException(openCloseException);
		} finally {
			currentTransaction.remove();
		}
	}

	private void restoreAutocommit(Connection connection) {
		try {
			connection.setAutoCommit(true);
		} catch (SQLException e) {
			throw new SQLRuntimeException("Exception when restoring autocommit on connection. Transaction is already committed, but connection might be broken afterwards.", e);
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
			throw new SQLRuntimeException(rollbackException);
		} catch (RuntimeException rollbackException) {
			LOG.error("Original application exception overridden by rollback-exception. Throwing rollback-exception. Original application exception: ", originalException);
			throw rollbackException;
		}
	}

	public interface DoInTransaction<T> {
		T doInTransaction();
	}
}
