package org.denspbru;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.jdbc.JdbcMeta;

import java.sql.*;
import java.util.Collections;

public class DuckMeta extends JdbcMeta {

    public DuckMeta() throws SQLException {
        super("jdbc:duckdb:");
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle h, String sql,
                                           long maxRowCount, PrepareCallback callback) throws NoSuchStatementException {
        Statement stmt = null;
        try {
            stmt = super.getConnection(h.connectionId).createStatement();

            boolean hasResultSet = stmt.execute(sql);

            if (!hasResultSet) {
                Signature signature = new Signature(
                        Collections.emptyList(),    // columns
                        sql,                        // sql
                        Collections.emptyList(),    // parameters
                        Collections.emptyMap(),     // internal parameters
                        Meta.CursorFactory.OBJECT,  // обязательно! иначе client падает
                        StatementType.OTHER_DDL
                );

                // Обработка DDL и DML (insert/update/create/drop)
                return new ExecuteResult(Collections.singletonList(
                        MetaResultSet.create(
                                h.connectionId,
                                h.id, // Signature (для DDL — отсутствует)\
                                true,
                                signature,
                                Meta.Frame.EMPTY
                        )
                ));
            }

            // по умолчанию вызываем супер реализацию для SELECT и других
            return super.prepareAndExecute(h, sql, maxRowCount, callback);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}