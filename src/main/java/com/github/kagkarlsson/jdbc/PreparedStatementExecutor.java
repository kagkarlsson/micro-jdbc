/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementExecutor<T> {

  T execute(PreparedStatement preparedStatement) throws SQLException;

  PreparedStatementExecutor<Boolean> EXECUTE =
      new PreparedStatementExecutor<Boolean>() {
        @Override
        public Boolean execute(PreparedStatement preparedStatement) throws SQLException {
          return preparedStatement.execute();
        }
      };

  PreparedStatementExecutor<int[]> EXECUTE_BATCH =
      new PreparedStatementExecutor<int[]>() {
        @Override
        public int[] execute(PreparedStatement preparedStatement) throws SQLException {
          return preparedStatement.executeBatch();
        }
      };
}
