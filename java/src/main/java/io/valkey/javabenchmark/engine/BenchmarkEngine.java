/*
 * Copyright 2025 the original author or authors.
 */
package io.valkey.javabenchmark.engine;

import io.valkey.javabenchmark.client.BenchmarkClient;
import io.valkey.javabenchmark.client.BenchmarkClientFactory;
import io.valkey.javabenchmark.command.Command;
import io.valkey.javabenchmark.command.Command.CommandResult;
import io.valkey.javabenchmark.command.CommandFactory;
import io.valkey.javabenchmark.config.*;
import io.valkey.javabenchmark.metrics.IntervalReporter;
import io.valkey.javabenchmark.metrics.NdjsonMetricsWriter;
import io.valkey.javabenchmark.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main benchmark engine that orchestrates the benchmark execution.
 * 
 * <p>Uses a <b>virtual-thread-per-client</b> architecture where each client connection
 * is owned by a long-lived virtual thread that runs a tight request loop. This design
 * eliminates the overhead of per-request VT creation and scheduling that limited the
 * previous architecture to ~400K RPS.</p>
 * 
 * <h3>Architecture</h3>
 * <pre>
 *   Main thread
 *     │
 *     ├── Creates N client connections
 *     ├── Spawns N long-lived virtual threads (one per client)
 *     │     │
 *     │     ├── VT-0: while(running) { execute → join → record → repeat }
 *     │     ├── VT-1: while(running) { execute → join → record → repeat }
 *     │     ├── ...
 *     │     └── VT-N: while(running) { execute → join → record → repeat }
 *     │
 *     ├── Logs progress periodically
 *     └── Waits for all VTs to complete, merges metrics
 * </pre>
 * 
 * <p>Key advantages over the previous command-issuer + semaphore design:</p>
 * <ul>
 *   <li><b>No per-request VT creation</b> — eliminates 400K+ VT spawns/sec overhead</li>
 *   <li><b>No semaphore contention</b> — backpressure is implicit (VT blocks on I/O)</li>
 *   <li><b>No CompletableFuture chains</b> — direct join() parks VT, not carrier</li>
 *   <li><b>Fewer carrier threads needed</b> — VTs park on I/O, carriers are free</li>
 *   <li><b>Per-VT state</b> — KeyGenerator, CommandSelector are thread-local, zero contention</li>
 * </ul>
 *
 * @author Ilia Kolominsky
 */
public class BenchmarkEngine {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkEngine.class);
    
    /** Default pipeline depth (max in-flight requests per client) */
    private static final int DEFAULT_PIPELINE_DEPTH = 1;
    
    /** Progress logging interval in seconds */
    private static final int PROGRESS_LOG_INTERVAL_SECONDS = 10;

    private final String host;
    private final int port;
    private final DriverConfig driverConfig;
    private final WorkloadConfig workloadConfig;
    private final NdjsonMetricsWriter metricsWriter;
    private final String metricsPath;
    private final String commitId;
    
    // Client reference for version info
    private BenchmarkClient sampleClient;

    public BenchmarkEngine(String host, int port, DriverConfig driverConfig,
                          WorkloadConfig workloadConfig, String metricsPath) {
        this(host, port, driverConfig, workloadConfig, metricsPath, null, null);
    }

    public BenchmarkEngine(String host, int port, DriverConfig driverConfig,
                          WorkloadConfig workloadConfig, String metricsPath, String commitId) {
        this(host, port, driverConfig, workloadConfig, metricsPath, commitId, null);
    }

    public BenchmarkEngine(String host, int port, DriverConfig driverConfig,
                          WorkloadConfig workloadConfig, String metricsPath, String commitId,
                          Integer commandIssuerThreads) {
        this.host = host;
        this.port = port;
        this.driverConfig = driverConfig;
        this.workloadConfig = workloadConfig;
        this.metricsWriter = new NdjsonMetricsWriter(metricsPath);
        this.metricsPath = metricsPath;
        this.commitId = commitId;
    }

    public void run() throws Exception {
        logger.info("Starting benchmark: {}", workloadConfig.getBenchmarkProfile().getName());
        logger.info("Driver: {}, Mode: {}", driverConfig.getDriverId(), driverConfig.getMode());
        logger.info("Server: {}:{}", host, port);

        // Set up metadata using a sample client to get version info
        setupMetadata();

        for (PhaseConfig phase : workloadConfig.getPhases()) {
            executePhase(phase);
        }

        logger.info("Benchmark completed");
    }

    /**
     * Set up metadata for metrics output, including driver versions.
     */
    private void setupMetadata() {
        try {
            sampleClient = BenchmarkClientFactory.createAndConnect(host, port, driverConfig);
            
            String driverId = driverConfig.getDriverId();
            String primaryVersion = sampleClient.getDriverVersion();
            String secondaryDriverId = driverConfig.getSecondaryDriverId();
            String secondaryVersion = sampleClient.getSecondaryDriverVersion();
            
            metricsWriter.setMetadata(commitId, driverId, primaryVersion, secondaryDriverId, secondaryVersion);
            
            logger.info("Metadata: commit={}, driver={}, version={}", 
                        commitId != null ? commitId : "N/A", driverId, primaryVersion);
            
            sampleClient.close();
            sampleClient = null;
        } catch (Exception e) {
            logger.warn("Failed to get driver version for metadata: {}", e.getMessage());
            metricsWriter.setMetadata(commitId, driverConfig.getDriverId(), "unknown", 
                                     driverConfig.getSecondaryDriverId(), null);
        }
    }

    private void executePhase(PhaseConfig phase) throws Exception {
        logger.info("=== Starting phase: {} ({}) ===", phase.getId(), phase.getDescription());

        int pipelineDepth = phase.getPipelineDepth() > 0 ? phase.getPipelineDepth() : DEFAULT_PIPELINE_DEPTH;
        
        // Create client connections
        List<BenchmarkClient> clients = createClients(phase);
        
        // Create commands
        List<Command> commands = CommandFactory.createAll(phase.getCommands());
        
        // Create key generator (base — each VT will fork its own)
        KeyGenerator keyGenerator = KeyGenerator.create(phase.getKeyspace());
        
        // Create rate limiter
        RateLimiter rateLimiter = phase.hasRpsLimit() ? 
                RateLimiter.create(phase.getRpsLimit()) : null;
        
        // Create metrics collector
        MetricsCollector metrics = new MetricsCollector();

        // Warmup: send PINGs on all clients (blocking, before measured workload)
        warmupClients(clients, phase.getWarmupRequests());

        // Interval reporting (soak tests): sidecar is created lazily, only for phases that enable it
        IntervalReporter intervalReporter = null;
        if (phase.hasReportingInterval()) {
            String sidecar = IntervalReporter.sidecarPath(metricsPath);
            intervalReporter = new IntervalReporter(Path.of(sidecar), phase.getId(),
                    phase.getConnections(), phase.getReportingIntervalSeconds());
            intervalReporter.open();
            metrics.setIntervalReporter(intervalReporter);
            logger.info("Interval reporting every {}s to {}", phase.getReportingIntervalSeconds(), sidecar);
        }

        // Execute workload with VT-per-client architecture
        String status = executeWorkload(phase, clients, commands, keyGenerator, 
                                         rateLimiter, metrics, pipelineDepth, intervalReporter);

        // Write metrics
        metricsWriter.writePhaseResults(phase.getId(), status, phase.getConnections(), metrics);

        // Close clients
        closeClients(clients);

        logger.info("=== Phase {} completed: {} ===", phase.getId(), status);
    }

    private List<BenchmarkClient> createClients(PhaseConfig phase) throws Exception {
        logger.info("Creating {} connections...", phase.getConnections());
        
        List<BenchmarkClient> clients = new ArrayList<>();
        RateLimiter cpsLimiter = phase.hasCpsLimit() ? 
                RateLimiter.create(phase.getCpsLimit()) : null;

        for (int i = 0; i < phase.getConnections(); i++) {
            if (cpsLimiter != null) {
                cpsLimiter.acquire();
            }
            
            BenchmarkClient client = BenchmarkClientFactory.createAndConnect(host, port, driverConfig);
            clients.add(client);
            
            if ((i + 1) % 50 == 0) {
                logger.info("Created {}/{} connections", i + 1, phase.getConnections());
            }
        }
        
        logger.info("All {} connections established", clients.size());
        return clients;
    }

    /**
     * Warmup: send PINGs on all clients to verify connections are alive.
     * 
     * @throws Exception if any warmup PING fails (phase should be aborted)
     */
    private void warmupClients(List<BenchmarkClient> clients, int warmupRequestsPerClient) 
            throws Exception {
        if (warmupRequestsPerClient <= 0) return;
        
        logger.info("Warming up {} clients with {} PING(s) each...", 
                    clients.size(), warmupRequestsPerClient);
        
        // Enable warmup mode so clients (e.g., RecordingBenchmarkClient) suppress error simulation
        for (BenchmarkClient client : clients) {
            client.setWarmupMode(true);
        }
        
        try {
            // Use VTs for parallel warmup
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<?>> futures = new ArrayList<>();
                for (BenchmarkClient client : clients) {
                    futures.add(executor.submit(() -> {
                        for (int r = 0; r < warmupRequestsPerClient; r++) {
                            client.ping().join(); // Let exceptions propagate
                        }
                    }));
                }
                // Wait for all warmup tasks — propagate any failure
                for (Future<?> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
            }
        } finally {
            // Disable warmup mode — error simulation resumes for the actual workload
            for (BenchmarkClient client : clients) {
                client.setWarmupMode(false);
            }
        }
        
        logger.info("Warmup completed");
    }

    private String executeWorkload(PhaseConfig phase, List<BenchmarkClient> clients,
                                   List<Command> commands, KeyGenerator keyGenerator,
                                   RateLimiter rateLimiter, MetricsCollector metrics,
                                   int pipelineDepth, IntervalReporter intervalReporter) {
        
        CompletionConfig completion = phase.getCompletion();
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong requestCount = new AtomicLong(0);
        
        int workerCount = clients.size();
        logger.info("Spawning {} worker virtual threads (pipeline_depth={})", 
                    workerCount, pipelineDepth);
        
        // Compute per-worker request budget for request-based completion
        long totalTargetRequests = completion.isRequestBased() ? completion.getTotalRequests() : -1;
        
        // Duration end time
        long endTimeMs = completion.isDurationBased() 
                ? System.currentTimeMillis() + (completion.getDurationSeconds() * 1000)
                : Long.MAX_VALUE;
        
        metrics.start();
        if (intervalReporter != null) {
            intervalReporter.start();
        }
        long startTime = System.currentTimeMillis();
        
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> workerFutures = new ArrayList<>(workerCount);
            
            for (int i = 0; i < workerCount; i++) {
                final int workerIndex = i;
                final BenchmarkClient client = clients.get(i);
                final KeyGenerator workerKeyGen = keyGenerator.forkForThread(workerIndex);
                final CommandSelector workerSelector = new CommandSelector(commands);
                
                workerFutures.add(executor.submit(() -> {
                    Thread.currentThread().setName("worker-" + workerIndex);
                    
                    try {
                        if (pipelineDepth <= 1) {
                            runSyncLoop(client, workerSelector, workerKeyGen, rateLimiter,
                                       metrics, requestCount, running, totalTargetRequests, endTimeMs);
                        } else {
                            runPipelinedLoop(client, workerSelector, workerKeyGen, rateLimiter,
                                            metrics, requestCount, running, totalTargetRequests, 
                                            endTimeMs, pipelineDepth);
                        }
                    } catch (Exception e) {
                        // Signal all workers to stop — fail-fast on unexpected errors
                        running.set(false);
                        if (e instanceof RuntimeException re) throw re;
                        throw new RuntimeException(e); // Wrap checked exceptions for Future.get()
                    }
                }));
            }
            
            // Progress logging from main thread while workers run
            long lastLogTime = System.currentTimeMillis();
            boolean allDone = false;
            
            while (!allDone) {
                allDone = true;
                for (Future<?> f : workerFutures) {
                    if (!f.isDone()) {
                        allDone = false;
                        break;
                    }
                }
                if (!allDone) {
                    Thread.sleep(100);
                    lastLogTime = logProgressIfNeeded(requestCount.get(), totalTargetRequests, 
                                                      lastLogTime, startTime);
                }
            }
            
            // Check for worker failures — any exception means the phase failed
            boolean hasWorkerFailures = false;
            for (Future<?> f : workerFutures) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    logger.error("Worker failed: {}", e.getCause().getMessage());
                    hasWorkerFailures = true;
                }
            }
            
            if (hasWorkerFailures) {
                logger.error("Phase failed: one or more workers crashed");
                return "ERROR";
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Workload interrupted");
            return "INTERRUPTED";
        } catch (Exception e) {
            logger.error("Error during workload execution: {}", e.getMessage());
            return "ERROR";
        } finally {
            metrics.stop();
            if (intervalReporter != null) {
                // Workers are done (executor closed): emit the final partial interval
                intervalReporter.stop();
                metrics.setIntervalReporter(null);
            }
        }
        
        logger.info("All operations completed. Total requests: {}", requestCount.get());
        return "COMPLETED";
    }

    /**
     * Synchronous request loop for pipeline_depth=1.
     * 
     * <p>Each VT owns one client and runs: execute → join → record → repeat.
     * The join() parks the VT (not the carrier), so the carrier is free to
     * run other VTs while waiting for the server response.</p>
     */
    private void runSyncLoop(BenchmarkClient client, CommandSelector selector,
                              KeyGenerator keyGen, RateLimiter rateLimiter,
                              MetricsCollector metrics, AtomicLong requestCount,
                              AtomicBoolean running, long totalTargetRequests, long endTimeMs) 
            throws InterruptedException {
        
        while (running.get()) {
            // Check termination conditions
            if (System.currentTimeMillis() >= endTimeMs) break;
            if (totalTargetRequests > 0) {
                long claimed = requestCount.getAndIncrement();
                if (claimed >= totalTargetRequests) {
                    requestCount.decrementAndGet();
                    break;
                }
            }
            
            // Rate limiting (parks VT, not carrier)
            if (rateLimiter != null) {
                rateLimiter.acquire();
            }
            
            // Select and execute command
            Command command = selector.select();
            try {
                CommandResult result = command.execute(client, keyGen).join();
                metrics.record(result);
            } catch (CompletionException e) {
                // Command execution failed
                metrics.record(CommandResult.failure(command.getName(), e.getMessage()));
            }
            
            // For duration-based: count here (after execution)
            if (totalTargetRequests <= 0) {
                requestCount.incrementAndGet();
            }
        }
    }

    /**
     * Pipelined request loop for pipeline_depth > 1.
     * 
     * <p>Each VT owns one client and maintains up to pipeline_depth in-flight requests.
     * Uses CompletableFuture.anyOf() to wait for the next completion, then immediately
     * issues a new request to keep the pipeline full.</p>
     */
    private void runPipelinedLoop(BenchmarkClient client, CommandSelector selector,
                                   KeyGenerator keyGen, RateLimiter rateLimiter,
                                   MetricsCollector metrics, AtomicLong requestCount,
                                   AtomicBoolean running, long totalTargetRequests,
                                   long endTimeMs, int pipelineDepth)
            throws InterruptedException {
        
        // Track pending futures with their command names for error reporting
        List<CompletableFuture<CommandResult>> pending = new ArrayList<>(pipelineDepth);
        
        // Fill the pipeline initially
        for (int i = 0; i < pipelineDepth && running.get(); i++) {
            if (!claimAndSubmit(client, selector, keyGen, rateLimiter, requestCount,
                               totalTargetRequests, endTimeMs, pending)) {
                break;
            }
        }
        
        // Event loop: wait for completion, record, refill
        while (!pending.isEmpty() && running.get()) {
            // Wait for any future to complete
            try {
                CompletableFuture.anyOf(pending.toArray(new CompletableFuture[0])).join();
            } catch (CompletionException e) {
                // At least one failed — we'll handle it below
            }
            
            // Process all completed futures
            Iterator<CompletableFuture<CommandResult>> it = pending.iterator();
            while (it.hasNext()) {
                CompletableFuture<CommandResult> f = it.next();
                if (f.isDone()) {
                    it.remove();
                    try {
                        CommandResult result = f.join();
                        metrics.record(result);
                    } catch (CompletionException e) {
                        metrics.record(CommandResult.failure("UNKNOWN", e.getMessage()));
                    }
                    
                    // For duration-based: count completions
                    if (totalTargetRequests <= 0) {
                        requestCount.incrementAndGet();
                    }
                    
                    // Refill the pipeline slot
                    if (running.get()) {
                        claimAndSubmit(client, selector, keyGen, rateLimiter, requestCount,
                                      totalTargetRequests, endTimeMs, pending);
                    }
                }
            }
        }
    }

    /**
     * Claim a request slot (for request-based) and submit a new request.
     * Returns false if the phase should end.
     */
    private boolean claimAndSubmit(BenchmarkClient client, CommandSelector selector,
                                    KeyGenerator keyGen, RateLimiter rateLimiter,
                                    AtomicLong requestCount, long totalTargetRequests,
                                    long endTimeMs, List<CompletableFuture<CommandResult>> pending)
            throws InterruptedException {
        
        if (System.currentTimeMillis() >= endTimeMs) return false;
        
        if (totalTargetRequests > 0) {
            long claimed = requestCount.getAndIncrement();
            if (claimed >= totalTargetRequests) {
                requestCount.decrementAndGet();
                return false;
            }
        }
        
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }
        
        Command command = selector.select();
        pending.add(command.execute(client, keyGen));
        return true;
    }

    private void closeClients(List<BenchmarkClient> clients) {
        logger.info("Closing {} connections...", clients.size());
        for (BenchmarkClient client : clients) {
            try {
                client.close();
            } catch (Exception e) {
                logger.warn("Error closing client: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Log progress if enough time has elapsed since last log.
     */
    private long logProgressIfNeeded(long current, long target, long lastLogTime, long startTime) {
        long now = System.currentTimeMillis();
        if (now - lastLogTime < PROGRESS_LOG_INTERVAL_SECONDS * 1000) {
            return lastLogTime;
        }
        
        long elapsedMs = now - startTime;
        long rate = elapsedMs > 0 ? (current * 1000 / elapsedMs) : 0;
        
        if (target > 0) {
            double percent = (current * 100.0 / target);
            logger.info("Progress: {}/{} requests ({} %) - {} req/s", 
                       current, target, String.format("%.1f", percent), rate);
        } else {
            logger.info("Progress: {} requests - {} req/s", current, rate);
        }
        
        return now;
    }

    /**
     * Selects commands based on their weights.
     */
    private static class CommandSelector {
        private final List<Command> commands;
        private final double[] cumulativeWeights;
        private final Random random = new Random();

        CommandSelector(List<Command> commands) {
            this.commands = commands;
            this.cumulativeWeights = new double[commands.size()];
            
            double sum = 0;
            for (int i = 0; i < commands.size(); i++) {
                sum += commands.get(i).getWeight();
                cumulativeWeights[i] = sum;
            }
        }

        Command select() {
            double r = random.nextDouble();
            for (int i = 0; i < cumulativeWeights.length; i++) {
                if (r <= cumulativeWeights[i]) {
                    return commands.get(i);
                }
            }
            return commands.get(commands.size() - 1);
        }
    }
}
