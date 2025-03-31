package org.denspbru;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.*;
import java.util.Map;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        // 1. Подключение к PostgreSQL
        Connection pgConn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test",
                "test",
                "test");
        Statement stmt = pgConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM test"); // замените на свою таблицу

        // 2. Преобразование в Arrow-таблицу
        Map<String, FieldVector> columns = PostgresToArrow.fromResultSet(rs, new RootAllocator());
        ArrowTable table = new ArrowTable(columns);

        // 3. Calcite schema и выполнение SQL
        Properties info = new Properties();
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("TEST", table);

        Statement sqlStmt = connection.createStatement();
        ResultSet resultSet = sqlStmt.executeQuery("SELECT NAME FROM TEST WHERE id > 2");
        while (resultSet.next()) {
            System.out.println("Name: " + resultSet.getString(1));
        }

        connection.close();
        pgConn.close();
    }
}
