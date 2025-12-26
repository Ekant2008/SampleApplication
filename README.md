# DazzleDuck SQL – Simple Example

This project contains a minimal example of using DazzleDuck SQL to collect application logs and store them as Arrow/Parquet files.

What This Example Does
- Starts a local DazzleDuck SQL HTTP server
- Runs a sample Java application
- Writes application logs to Parquet files in a local warehouse
  

## SampleApplication

- Generates simple log messages
- Uses ArrowSimpleLogger for demonstration
- Explicitly flushes logs using logger.close()


## How to Run
Start the server
- mvn exec:java -Dexec.mainClass=io.dazzleduck.sql.example.server.ServerLauncher

Run the application
- mvn exec:java -Dexec.mainClass=io.dazzleduck.sql.example.SampleApplication

Check the logs
- Parquet files are created in /tmp/logs

### Requirements
- Java 21+
- DazzleDuck SQL runtime
- Apache Arrow 18.x