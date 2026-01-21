package io.dazzleduck.sql.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.micrometer.MicrometerForwarder;
import io.dazzleduck.sql.micrometer.config.MicrometerForwarderConfig;
import io.micrometer.core.instrument.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SampleApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleApplication.class);
    private static final Random random = new Random();
    private static final AtomicInteger activeUsers = new AtomicInteger(0);
    private static final AtomicInteger queueSize = new AtomicInteger(0);

    public static void main(String[] args) {

        Config root = ConfigFactory.load();
        Config cfg = root.getConfig("dazzleduck_micrometer");

        MicrometerForwarderConfig config =
                MicrometerForwarderConfig.builder()
                        .baseUrl(cfg.getConfig("http").getString("base_url"))
                        .username(cfg.getConfig("http").getString("username"))
                        .password(cfg.getConfig("http").getString("password"))
                        .targetPath(cfg.getConfig("http").getString("target_path"))
                        .httpClientTimeout(
                                Duration.ofMillis(
                                        cfg.getConfig("http").getLong("http_client_timeout_ms")
                                )
                        )
                        .stepInterval(Duration.ofMillis(cfg.getLong("step_interval_ms")))
                        .minBatchSize(cfg.getLong("min_batch_size"))
                        .maxBatchSize(cfg.getLong("max_batch_size"))
                        .maxSendInterval(Duration.ofMillis(cfg.getLong("max_send_interval_ms")))
                        .maxInMemorySize(cfg.getLong("max_in_memory_bytes"))
                        .maxOnDiskSize(cfg.getLong("max_on_disk_bytes"))
                        .retryCount(cfg.getInt("retry_count"))
                        .retryIntervalMillis(cfg.getLong("retry_interval_ms"))
                        .project(cfg.getStringList("projections"))
                        .enabled(cfg.getBoolean("enabled"))
                        .build();

        MicrometerForwarder forwarder = MicrometerForwarder.createAndStart(config);
        MeterRegistry registry = forwarder.getRegistry();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);

        try {
            LOGGER.info("Sample application started - generating diverse metrics");

            // Register all metric generators
            registerGauges(registry);

            // Schedule continuous metric generation
            scheduler.scheduleAtFixedRate(() -> generateCounterMetrics(registry), 0, 2, TimeUnit.SECONDS);
            scheduler.scheduleAtFixedRate(() -> generateTimerMetrics(registry), 0, 3, TimeUnit.SECONDS);
            scheduler.scheduleAtFixedRate(() -> generateDistributionMetrics(registry), 0, 5, TimeUnit.SECONDS);

            // Run for 60 seconds to generate data
            Thread.sleep(60000);

            LOGGER.info("Sample application finished");
            Thread.sleep(2000);

        } catch (Exception e) {
            LOGGER.error("Application error", e);
        } finally {
            scheduler.shutdown();
            try {
                forwarder.close();
            } catch (Exception e) {
                LOGGER.warn("Error closing forwarder", e);
            }
        }
    }

    /**
     * Register gauge metrics that track current values
     */
    private static void registerGauges(MeterRegistry registry) {
        // Gauge for active users (fluctuates)
        Gauge.builder("system.active.users", activeUsers, AtomicInteger::get)
                .description("Number of currently active users")
                .tag("service", "auth")
                .register(registry);

        // Gauge for queue size
        Gauge.builder("queue.size", queueSize, AtomicInteger::get)
                .description("Current queue size")
                .tag("queue", "processing")
                .register(registry);

        // Gauge for JVM memory usage
        Gauge.builder("jvm.memory.used", Runtime.getRuntime(),
                        runtime -> runtime.totalMemory() - runtime.freeMemory())
                .description("JVM memory used in bytes")
                .baseUnit("bytes")
                .register(registry);

        // Gauge for CPU load simulation
        Gauge.builder("system.cpu.usage", () -> random.nextDouble() * 100)
                .description("Simulated CPU usage percentage")
                .baseUnit("percent")
                .tag("host", "server-01")
                .register(registry);
    }

    /**
     * Generate various counter metrics
     */
    private static void generateCounterMetrics(MeterRegistry registry) {
        // HTTP request counter with different status codes
        String[] statusCodes = {"200", "201", "400", "404", "500"};
        String[] endpoints = {"/api/users", "/api/orders", "/api/products", "/api/auth"};

        for (String endpoint : endpoints) {
            String status = statusCodes[random.nextInt(statusCodes.length)];
            int weight = status.equals("200") ? 8 : 1; // 200s are more common

            if (random.nextInt(10) < weight) {
                Counter.builder("http.requests.total")
                        .description("Total HTTP requests")
                        .tag("endpoint", endpoint)
                        .tag("status", status)
                        .tag("method", "GET")
                        .register(registry)
                        .increment();
            }
        }

        // Database query counter
        String[] operations = {"SELECT", "INSERT", "UPDATE", "DELETE"};
        Counter.builder("database.queries.total")
                .description("Total database queries")
                .tag("operation", operations[random.nextInt(operations.length)])
                .tag("database", "postgresql")
                .register(registry)
                .increment(random.nextInt(5) + 1);

        // Business metrics - orders and revenue
        if (random.nextInt(10) < 3) {
            Counter.builder("orders.completed")
                    .description("Number of completed orders")
                    .tag("region", random.nextBoolean() ? "US" : "EU")
                    .register(registry)
                    .increment();
        }

        // Error counter
        if (random.nextInt(20) == 0) {
            Counter.builder("errors.total")
                    .description("Total errors")
                    .tag("type", random.nextBoolean() ? "validation" : "timeout")
                    .tag("severity", random.nextBoolean() ? "warning" : "error")
                    .register(registry)
                    .increment();
        }

        // Update gauge values
        activeUsers.set(50 + random.nextInt(100));
        queueSize.set(random.nextInt(500));
    }

    /**
     * Generate timer metrics for latency tracking
     */
    private static void generateTimerMetrics(MeterRegistry registry) {
        // API endpoint latency
        String[] endpoints = {"/api/users", "/api/orders", "/api/products"};

        for (String endpoint : endpoints) {
            Timer timer = Timer.builder("http.request.duration")
                    .description("HTTP request duration")
                    .tag("endpoint", endpoint)
                    .register(registry);

            // Simulate varying latencies
            long latency = 50 + random.nextInt(200);
            if (random.nextInt(20) == 0) {
                latency = 1000 + random.nextInt(2000); // Occasional slow request
            }

            timer.record(Duration.ofMillis(latency));
        }

        // Database query latency
        Timer.builder("database.query.duration")
                .description("Database query execution time")
                .tag("operation", "SELECT")
                .register(registry)
                .record(Duration.ofMillis(10 + random.nextInt(100)));

        // Cache access time
        Timer.builder("cache.access.duration")
                .description("Cache access time")
                .tag("cache", "redis")
                .tag("result", random.nextBoolean() ? "hit" : "miss")
                .register(registry)
                .record(Duration.ofMillis(1 + random.nextInt(10)));
    }

    /**
     * Generate distribution summary metrics
     */
    private static void generateDistributionMetrics(MeterRegistry registry) {
        // Request payload size
        DistributionSummary.builder("http.request.size")
                .description("HTTP request payload size")
                .baseUnit("bytes")
                .tag("endpoint", "/api/upload")
                .register(registry)
                .record(1024 + random.nextInt(10240));

        // Response size
        DistributionSummary.builder("http.response.size")
                .description("HTTP response payload size")
                .baseUnit("bytes")
                .tag("endpoint", "/api/users")
                .register(registry)
                .record(512 + random.nextInt(5120));

        // Order value distribution
        DistributionSummary.builder("order.value")
                .description("Order value in dollars")
                .baseUnit("dollars")
                .tag("currency", "USD")
                .register(registry)
                .record(10 + random.nextDouble() * 500);

        // Items per order
        DistributionSummary.builder("order.items.count")
                .description("Number of items per order")
                .register(registry)
                .record(1 + random.nextInt(10));
    }
}