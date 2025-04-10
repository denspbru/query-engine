package org.denspbru;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.Meta;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.Servlet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;
public class JDBCServer {
    public static void main(String[] args) throws Exception {
        // 1. Загружаем таблицы из PostgreSQL в InMemoryStore
        Connection pgConn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/test",
                "test",
                "test"
        );

        InMemoryStore store = InMemoryStore.INSTANCE;

        // Загрузка таблицы sales
        ResultSet rsSales = pgConn.createStatement().executeQuery("SELECT * FROM sales");
        Map<String, FieldVector> salesVectors = PostgresToArrow.fromResultSet(rsSales, new RootAllocator());
        store.register("sales", new Table("sales", salesVectors));

        // Загрузка таблицы product_type
        ResultSet rsTypes = pgConn.createStatement().executeQuery("SELECT * FROM product_type");
        Map<String, FieldVector> typeVectors = PostgresToArrow.fromResultSet(rsTypes, new RootAllocator());
        store.register("product_type", new Table("product_type", typeVectors));

        pgConn.close();

        // 2. Запускаем JDBC-сервер через Avatica + MyMeta
        int port = 8765;
        System.out.println("Starting Avatica JDBC server on port " + port);
        try {
            Meta meta = new MyMeta(); // твоя реализация
            LocalService service = new LocalService(meta);
            AvaticaJsonHandler handler = new AvaticaJsonHandler(service);

            Server server = new Server(port);
            ServletContextHandler context = new ServletContextHandler(server, "/");

            ServletHolder servletHolder = new ServletHolder();
            servletHolder.setServlet(handler); // ✅ правильный способ
            context.addServlet(servletHolder, "/*");

            server.start();
            System.out.println("✅ JDBC сервер работает на http://localhost:" + port);
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
