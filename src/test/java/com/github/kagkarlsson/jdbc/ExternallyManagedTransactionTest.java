package com.github.kagkarlsson.jdbc;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import static com.github.kagkarlsson.jdbc.PreparedStatementSetter.NOOP;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class ExternallyManagedTransactionTest {

    @Rule
    public HsqlTestDatabaseRule database = new HsqlTestDatabaseRule();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private JdbcRunner internalTransactions;
    private JdbcRunner externalTransacitons;

    @Before
    public void setUp() {
        internalTransactions = new JdbcRunner(new DisableAutoCommit(database.getDataSource()), false);
        externalTransacitons = new JdbcRunner(new DisableAutoCommit(database.getDataSource()), true);
    }

    @Test
    public void test_externally_managed_transaction() {
        internalTransactions.execute("create table table1 ( column1 INT);", NOOP);
        assertThat(internalTransactions.execute("insert into table1(column1) values (?)", setInt(1)), is(1));
        assertThat(internalTransactions.query("select * from table1", PreparedStatementSetter.NOOP, Mappers.SINGLE_INT), is(1));

        externalTransacitons.execute("update table1 set column1 = ?", setInt(5));
        // should not have been committed
        assertThat(internalTransactions.query("select * from table1", PreparedStatementSetter.NOOP, Mappers.SINGLE_INT), is(1));

        internalTransactions.execute("update table1 set column1 = ?", setInt(2));
        // updated
        assertThat(internalTransactions.query("select * from table1", PreparedStatementSetter.NOOP, Mappers.SINGLE_INT), is(2));
    }

    private PreparedStatementSetter setInt(int value) {
        return ps -> ps.setInt(1, value);
    }

    private static class DisableAutoCommit implements DataSource {
        private final DataSource underlying;

        DisableAutoCommit(DataSource underlying) {
            this.underlying = underlying;
        }

        @Override
        public Connection getConnection() throws SQLException {
            Connection c = underlying.getConnection();
            c.setAutoCommit(false);
            return c;
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return underlying.getLogWriter();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            underlying.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            underlying.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return underlying.getLoginTimeout();
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return underlying.getParentLogger();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return underlying.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return underlying.isWrapperFor(iface);
        }
    }

}
