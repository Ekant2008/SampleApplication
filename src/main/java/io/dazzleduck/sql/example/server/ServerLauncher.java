package io.dazzleduck.sql.example.server;

import java.util.UUID;

public class ServerLauncher {

    public static void main(String[] args) throws Exception {
        String warehousePath = "/tmp/dazzleduckWarehouse/metric";
        new java.io.File(warehousePath).mkdirs();
        String warehousePath2 = "/tmp/dazzleduckWarehouse/log";
        new java.io.File(warehousePath2).mkdirs();
        int port = 8081;
        String warehouse = "/tmp/dazzleduckWarehouse";

        io.dazzleduck.sql.runtime.Main.main(new String[] {
                "--conf", "dazzleduck_server.networking_modes=[http]",
                "--conf", "dazzleduck_server.http.port=" + port,
                "--conf", "dazzleduck_server.warehouse=" + warehouse,
                "--conf", "dazzleduck_server.ingestion.max_delay_ms=500",
                "--conf", "dazzleduck_server.startup_script_provider.content="
                + "INSTALL arrow FROM community; LOAD arrow;"
        });

        System.out.println("DazzleDuck server started");
    }
}
