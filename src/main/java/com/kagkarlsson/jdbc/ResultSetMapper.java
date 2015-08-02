package com.kagkarlsson.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultSetMapper<T>{

	T map(ResultSet resultSet) throws SQLException;
}
