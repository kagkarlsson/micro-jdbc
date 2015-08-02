package com.kagkarlsson.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

public interface ResultSetMapper<T>{

	T map(ResultSet resultSet) throws SQLException;

}
