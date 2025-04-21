package org.denspbru;

import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta.*;


import java.sql.*;
import java.util.*;

public class MyMeta extends JdbcMeta {
    private static final Logger logger = LoggerFactory.getLogger(MyMeta.class);

    public MyMeta() throws SQLException {
        super(org.apache.calcite.avatica.remote.Driver.CONNECT_STRING_PREFIX + "jdbc:calcite:");
    }

    @Override
    protected Connection createConnection(String url, Properties info) throws SQLException {
        // Устанавливаем соединение с Calcite
        Properties props = new Properties();
        props.setProperty("lex", "JAVA");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // Динамически создаем Calcite-схему с таблицами из InMemoryStore
        Schema dynamicSchema = new AbstractSchema() {
            @Override
            protected Map<String, org.apache.calcite.schema.Table> getTableMap() {
                Map<String, org.apache.calcite.schema.Table> tables = new HashMap<>();
                InMemoryStore store = InMemoryStore.INSTANCE;

                for (Map.Entry<String, Table> entry : store.getTables().entrySet()) {
                    String tableName = entry.getKey();
                    Table internalTable = entry.getValue();

                    // Оборачиваем Table в ArrowTable
                    ArrowTable arrowTable = new ArrowTable(internalTable.getColumns());
                    tables.put(tableName, arrowTable);
                }

                return tables;
            }
        };

        rootSchema.add("myschema", dynamicSchema);
        calciteConnection.setSchema("myschema");

        return connection;
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle handle, String sql, long maxRowCount,
                                           PrepareCallback callback) {
        try {
            logger.info("Выполнение запроса: {}", sql);

            // Получаем JDBC-соединение через API JdbcMeta
            Connection conn = getConnection(handle.connectionId);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            // Получаем метаданные результата
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            List<ColumnMetaData> columns = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                int ordinal = i - 1;
                boolean autoIncrement = false;
                boolean caseSensitive = true;
                boolean searchable = true;
                boolean currency = false;
                int displaySize = metaData.getColumnDisplaySize(i);
                String label = metaData.getColumnLabel(i);
                String columnName = metaData.getColumnName(i);
                String schemaName = metaData.getSchemaName(i);
                String tableName = metaData.getTableName(i);
                String catalogName = metaData.getCatalogName(i);
                int precision = metaData.getPrecision(i);
                int scale = metaData.getScale(i);
                int sqlType = metaData.getColumnType(i);
                String typeName = metaData.getColumnTypeName(i);
                ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(Class.forName(metaData.getColumnClassName(i)));

                // Правильная обёртка типа
                ColumnMetaData.AvaticaType avaticaType =
                        new ColumnMetaData.ScalarType(sqlType, typeName, rep);

                // Конструируем ColumnMetaData
                ColumnMetaData columnMetaData = new ColumnMetaData(
                        ordinal,
                        autoIncrement,
                        caseSensitive,
                        searchable,
                        currency,
                        displaySize,
                        false,
                        displaySize,
                        schemaName,
                        tableName,
                        catalogName,
                        precision,
                        scale,
                        typeName,
                        columnName, // возможно, здесь должен быть alias
                        avaticaType,
                        true, // nullable
                        false,
                        false,
                        null // формат (обычно не используется)
                );
                columns.add(columnMetaData);
            }

            Signature signature = new Signature(
                    columns,
                    sql,
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    Meta.CursorFactory.ARRAY,
                    StatementType.SELECT
            );

            Frame firstFrame = frame(rs, 0, Integer.MAX_VALUE);

            return new ExecuteResult(Collections.singletonList(
                    MetaResultSet.create(handle.connectionId, handle.id, true, signature, firstFrame)
            ));

        } catch (Exception e) {
            logger.error("Ошибка при выполнении запроса: {}", sql, e);
            throw new RuntimeException(e);
        }
    }


    public static Frame frame(ResultSet rs, int offset, int fetchMaxRowCount) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        List<Object> rows = new ArrayList<>();
        int rowCount = 0;

        while ((fetchMaxRowCount < 0 || rowCount < fetchMaxRowCount) && rs.next()) {
            Object[] row = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                row[i] = rs.getObject(i + 1);
            }
            rows.add(row); // Оборачиваем Object[] как Object
            rowCount++;
        }

        boolean done = !rs.next();
        return new Frame(offset, done, rows);
    }
}
