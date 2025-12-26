package io.dazzleduck.sql.example;

import io.dazzleduck.sql.logger.ArrowSimpleLogger;
import io.dazzleduck.sql.micrometer.metrics.MetricsRegistryFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;


public class SampleApplication {

    public static void main(String[] args) throws Exception {


        MeterRegistry registry = MetricsRegistryFactory.create();
        ArrowSimpleLogger logger = new ArrowSimpleLogger(SampleApplication.class.getName());//we have to use slf4j log not arrowlogger
      //  Logger logger = LoggerFactory.getLogger(SampleApplication.class);//this how we have to do

        try {
            logger.info("Sample application started");
            simulateWork(logger, registry);
            logger.info("Sample application finished successfully");

        } finally {
           registry.close();  // flush metrics
            logger.close();    // flush logs
        }
    }

    private static void simulateWork(
            ArrowSimpleLogger logger,
            MeterRegistry registry
    ) throws InterruptedException {

        Counter processedCounter = Counter.builder("records.count")
                        .description("Number of records processed")
                        .register(registry);

        Timer processingTimer = Timer.builder("record.time")
                        .description("Time spent processing records")
                        .register(registry);

        for (int i = 1; i <= 10; i++) {

            final int recordNumber = i;
            processingTimer.record(() -> {
                try {
                    logger.info("Processing record {}", recordNumber);
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });

            processedCounter.increment();
        }
    }
}
