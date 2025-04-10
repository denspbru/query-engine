package org.denspbru;

import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractSchema;


import java.sql.*;
import java.util.*;

public class MyMeta extends JdbcMeta {

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
}
