package org.denspbru;

import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class DuckJDBCServer {
    public static void main(String[] args) throws Exception {
        Server server = new Server(8765);
        server.setHandler(new AvaticaJsonHandler(new LocalService(new DuckMeta())));
        server.start();
        server.join();
    }
}
