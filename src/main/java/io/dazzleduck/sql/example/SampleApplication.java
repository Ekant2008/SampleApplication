package io.dazzleduck.sql.example;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Duration;

public class SampleApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleApplication.class);

    public static void main(String[] args) throws Exception {

        // Simple logging registry (prints to console)
        var registry = new LoggingMeterRegistry();

        try {
            LOGGER.info("Sample application started");
            simulateWork(LOGGER, registry);
            LOGGER.info("Sample application finished successfully");
            Thread.sleep(2000);

        } finally {
            registry.close();
        }
    }

    private static void simulateWork(Logger logger, MeterRegistry registry) {

        Counter processedCounter = Counter.builder("records.count")
                .description("Number of records processed")
                .register(registry);

        Timer processingTimer = Timer.builder("record.time")
                .description("Time spent processing records")
                .register(registry);

        for (int i = 1; i <= 10; i++) {
            int recordNumber = i;

            processingTimer.record(() -> {
                try {
                    logger.info("Processing record {}", recordNumber);
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Processing interrupted", e);
                }
            });

            processedCounter.increment();
        }
    }
}