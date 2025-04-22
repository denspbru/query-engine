package org.denspbru;

import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.TypedValue;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DuckMeta extends JdbcMeta {

    public DuckMeta() throws SQLException {
        super("jdbc:duckdb:");
        System.out.println("DuckMetaL: ");

    }
    @Override
    public Meta.ExecuteResult execute(Meta.StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame) throws NoSuchStatementException {
        System.out.println("execute(...) (not prepared)");
        return super.execute(h, parameterValues, maxRowsInFirstFrame);
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, PrepareCallback callback) throws NoSuchStatementException {
        try {
            System.out.println("Executing SQL: " + sql);
            Connection connection  = this.getConnection(h.connectionId);
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute(sql);
            if (hasResultSet) {
                ResultSet rs = statement.getResultSet();
                ResultSetMetaData metaData = rs.getMetaData();
                List<ColumnMetaData> columns = getColumnMetaData(metaData);
                Signature signature = new Signature(columns, sql, Collections.<AvaticaParameter>emptyList(), Collections.emptyMap(),CursorFactory.ARRAY, StatementType.SELECT);
                Frame firstFrame = toFrame(rs, 0, (int) maxRowCount);
                return new ExecuteResult(Collections.singletonList(MetaResultSet.create(h.connectionId, h.id, true, signature, firstFrame)));
            } else {
                int updateCount = statement.getUpdateCount();
                Signature signature = new Signature(Collections.emptyList(), sql, Collections.<AvaticaParameter>emptyList(),Collections.emptyMap(), CursorFactory.ARRAY, StatementType.OTHER_DDL);
                Frame emptyFrame = Frame.EMPTY;
                return new ExecuteResult(Collections.singletonList(MetaResultSet.create(h.connectionId, h.id, false, signature, emptyFrame)));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL: " + sql, e);
        }
    }
    public Frame toFrame(ResultSet rs, int offset, int fetchSize) throws SQLException {
        List<Object[]> rows = new ArrayList<>();
        int count = 0;
        boolean done = false;

        while (rs.next() && count < fetchSize) {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            Object[] row = new Object[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                row[i - 1] = rs.getObject(i);
            }
            rows.add(row);
            count++;
        }

        if (!rs.next()) {
            done = true;
        } else {
            // вернём курсор назад на одну строку
            rs.previous(); // DuckDB поддерживает scrollable ResultSet
        }

        return Frame.create(offset, done, Collections.singletonList(rows.toArray(new Object[0][])));
    }
    public List<ColumnMetaData> getColumnMetaData(ResultSetMetaData metaData) throws SQLException {
        List<ColumnMetaData> columns = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            ColumnMetaData.AvaticaType avaticaType = ColumnMetaData.scalar(
                    metaData.getColumnType(i),
                    metaData.getColumnTypeName(i),
                    ColumnMetaData.Rep.OBJECT  // безопасный дефолт
            );

            ColumnMetaData column = new ColumnMetaData(
                    i - 1,                                      // ordinal
                    false,                                      // autoIncrement
                    true,                                       // caseSensitive
                    true,                                       // searchable
                    false,                                      // currency
                    ResultSetMetaData.columnNullable,           // nullable
                    true,                                       // signed
                    metaData.getColumnDisplaySize(i),           // displaySize
                    metaData.getColumnLabel(i),                 // label
                    metaData.getColumnName(i),                  // columnName
                    metaData.getSchemaName(i),                  // schemaName
                    metaData.getPrecision(i),                   // precision
                    metaData.getScale(i),                       // scale
                    metaData.getTableName(i),                   // tableName
                    metaData.getCatalogName(i),                 // catalogName
                    avaticaType,                                // type
                    true,                                       // readOnly
                    false,                                      // writable
                    false,                                      // definitelyWritable
                    metaData.getColumnClassName(i)              // columnClassName
            );

            columns.add(column);  // ✅ ← это ты забыл
        }
        return columns;
    }


}