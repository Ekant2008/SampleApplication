package io.dazzleduck.sql.example;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.runtime.SharedTestServer;

import java.nio.file.Files;
import java.nio.file.Path;

public class DuckLakeMetricServer {

    private static final int HTTP_PORT = 8081;
    private static final int FLIGHT_PORT = 59307;

    private static final String CATALOG_NAME = "test_ducklake";
    private static final String SCHEMA_NAME = "main";
    private static final String TABLE_NAME = "test_metrics";
    private static final String DUCKLAKE_DATA_DIR = "ducklake_data";

    public static void main(String[] args) throws Exception {

        Path warehouse = Path.of(
                ConfigConstants.getWarehousePath(
                        ConfigFactory.load().getConfig(ConfigConstants.CONFIG_PATH)
                )
        );

        Files.createDirectories(warehouse);

        // Convert Windows path to forward slashes for DuckDB
        String warehousePath = warehouse.toAbsolutePath().toString().replace("\\", "/");

        String startupScript = """
    INSTALL arrow;
    LOAD arrow;

    LOAD ducklake;

    ATTACH 'ducklake:%s/%s.ducklake'
    AS %s
    (DATA_PATH '%s/%s');

    USE %s;

    CREATE TABLE IF NOT EXISTS %s (
        s_no BIGINT,
        timestamp TIMESTAMP,
        name VARCHAR,
        type VARCHAR,
        tags MAP(VARCHAR, VARCHAR),
        value DOUBLE,
        min DOUBLE,
        max DOUBLE,
        mean DOUBLE,
        application_host VARCHAR,
        date DATE
    );
""".formatted(
                warehousePath,
                CATALOG_NAME,
                CATALOG_NAME,
                warehousePath,
                DUCKLAKE_DATA_DIR,
                CATALOG_NAME,
                TABLE_NAME
        );


        System.out.println(startupScript);
        SharedTestServer server = getSharedTestServer(warehouse, startupScript);

        Files.createDirectories(Path.of(
                server.getWarehousePath(),
                DUCKLAKE_DATA_DIR,
                SCHEMA_NAME,
                TABLE_NAME
        ));


        System.out.println(" DuckLake Metric Server Started ");
        System.out.println(" HTTP  : http://localhost:" + HTTP_PORT);
        System.out.println(" Flight: grpc://localhost:" + FLIGHT_PORT);
        System.out.println(" Warehouse: " + warehouse);

        // Keep process alive
        Thread.currentThread().join();
    }

    private static SharedTestServer getSharedTestServer(Path warehouse, String startupScript) throws Exception {
        SharedTestServer server = new SharedTestServer();

        server.startWithWarehouse(
                HTTP_PORT,
                FLIGHT_PORT,
                "http.auth=none",
                "warehouse=" + warehouse.toAbsolutePath(),

                // DuckLake ingestion
                "ingestion_task_factory_provider.class=io.dazzleduck.sql.commons.ingestion.DuckLakeIngestionTaskFactoryProvider",

                // Mapping
                "ingestion_task_factory_provider.ingestion_queue_to_table_mapping.0.table_name=" + TABLE_NAME,
                "ingestion_task_factory_provider.ingestion_queue_to_table_mapping.0.schema_name=" + SCHEMA_NAME,
                "ingestion_task_factory_provider.ingestion_queue_to_table_mapping.0.catalog_name=" + CATALOG_NAME,
                "ingestion_task_factory_provider.ingestion_queue_to_table_mapping.0.ingestion_queue_id=metrics",

                // Startup SQL
                "startup_script_provider.class=io.dazzleduck.sql.flight.ConfigBasedStartupScriptProvider",
                "startup_script_provider.content=" + startupScript
        );
        return server;
    }
}