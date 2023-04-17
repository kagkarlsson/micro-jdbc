package com.github.kagkarlsson.jdbc;

import org.hsqldb.Database;
import org.hsqldb.DatabaseManager;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.sql.DataSource;

public class HsqlExtension implements BeforeEachCallback, AfterEachCallback {
    private JDBCDataSource dataSource;

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        final JDBCDataSource ds = new JDBCDataSource();
        ds.setUrl("jdbc:hsqldb:mem:jdbcrunner");
        ds.setUser("sa");

        dataSource = ds;
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        DatabaseManager.closeDatabases(Database.CLOSEMODE_IMMEDIATELY);
    }

    public DataSource getDataSource() {
        return dataSource;
    }
}
