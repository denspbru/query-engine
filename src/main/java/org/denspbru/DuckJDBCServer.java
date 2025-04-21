package org.denspbru;

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.LocalJsonService;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.sql.DriverManager;
import java.sql.Connection;

public class DuckJDBCServer {

    public static void main(String[] args) throws Exception {
        // Подключение к DuckDB
        Connection connection = DriverManager.getConnection("jdbc:duckdb:");

        // Meta → JdbcMeta (наследует org.apache.calcite.avatica.Meta)
        DuckMeta meta = new DuckMeta();

        // Json Service → принимает Meta, а не Service
//       LocalJsonService service = new LocalJsonService(meta);
        LocalService service = new LocalService(meta);

        // JSON handler → принимает Service
        AvaticaJsonHandler handler = new AvaticaJsonHandler(service);

        // Jetty сервер
        Server server = new Server(8765);
        server.setHandler(handler); // 👈 напрямую устанавливаем handler вместо сервлетов

        server.start();
        System.out.println("DuckDB JDBC-сервер (Jetty 11 + Avatica JSON) запущен на http://localhost:8765");
        server.join();
    }
}
