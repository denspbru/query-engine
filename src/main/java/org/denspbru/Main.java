package org.denspbru;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import java.util.List;

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
        ResultSet rs = stmt.executeQuery("SELECT * FROM sales"); // замените на свою таблицу

        // 2. Преобразование в Arrow-таблицу
        Map<String, FieldVector> columns = PostgresToArrow.fromResultSet(rs, new RootAllocator());
        ArrowTable table = new ArrowTable(columns);

        // 3. Calcite schema и выполнение SQL
        Properties info = new Properties();
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("SALES", table);



        Statement stmt2 = pgConn.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT * FROM product_type"); // замените на свою таблицу

        // 2. Преобразование в Arrow-таблицу
        Map<String, FieldVector> columns2 = PostgresToArrow.fromResultSet(rs2, new RootAllocator());
        ArrowTable table2 = new ArrowTable(columns2);

        // 3. Calcite schema и выполнение SQL
        rootSchema.add("PRODUCT_TYPE", table2);


        Statement sqlStmt = connection.createStatement();
        ResultSet resultSet = sqlStmt.executeQuery("SELECT t.product_type_name, count(*), sum(val) FROM SALES s left join PRODUCT_TYPE t on t.product_type_id = s.product_type_id  group by product_type_name");
        while (resultSet.next()) {
            System.out.println("product_type_id: " + resultSet.getString(1));
            System.out.println("count: " + resultSet.getString(2));
            System.out.println("sum: " + resultSet.getString(3));
        }


        Statement sqlStmtOlap = connection.createStatement();
        ResultSet resultSetOlap = sqlStmt.executeQuery("SELECT t.product_type_name,  s.val FROM SALES s left join PRODUCT_TYPE t on t.product_type_id = s.product_type_id ");

        Map<String, FieldVector> columns3 = PostgresToArrow.fromResultSet(resultSetOlap, new RootAllocator());
        ArrowTable table3 = new ArrowTable(columns3);

        Map<List<Object>, CubeBuilder.Agg> cube = CubeBuilder.buildCube(
                table3,
                List.of("PRODUCT_TYPE_NAME"),
                "VAL"
        );

        // Печатаем результат
        for (Map.Entry<List<Object>, CubeBuilder.Agg> entry : cube.entrySet()) {
            System.out.println("Group: " + entry.getKey() + " => " + entry.getValue());
        }

        connection.close();
        pgConn.close();
    }
}
