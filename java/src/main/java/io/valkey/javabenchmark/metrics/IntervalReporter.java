/*
 * Copyright 2025 the original author or authors.
 */
package io.valkey.javabenchmark.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.valkey.javabenchmark.command.Command.CommandResult;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Soak-test interval reporting (see docs/INTERVAL_REPORTING.md).
 *
 * <p>Receives every recorded result of a phase (in addition to the cumulative
 * {@link MetricsCollector}) into per-command, per-window HdrHistogram {@link Recorder}s, and a single
 * daemon scheduled thread snapshots and resets the window every N seconds, appending one
 * {@code "type":"interval"} NDJSON record to the sidecar file and flushing it.</p>
 *
 * <p>Hot path: one map lookup, two atomic increments and a wait-free {@link Recorder#recordValue}.</p>
 */
public final class IntervalReporter {
    private static final Logger logger = LoggerFactory.getLogger(IntervalReporter.class);

    private static final long MAX_LATENCY_MICROS = 600_000_000L;
    private static final String METRICS_SUFFIX = ".ndjson";
    private static final String INTERVAL_SUFFIX = ".interval.ndjson";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, WindowSlot> slots = new ConcurrentHashMap<>();
    private final Path outputPath;
    private final String phaseId;
    private final int connections;
    private final int intervalSeconds;

    private BufferedWriter writer;
    private ScheduledExecutorService scheduler;
    private long workloadStartMs;
    private long windowStartMs;
    private int index;

    public IntervalReporter(Path outputPath, String phaseId, int connections, int intervalSeconds) {
        if (intervalSeconds <= 0) {
            throw new IllegalArgumentException("intervalSeconds must be positive");
        }
        this.outputPath = outputPath;
        this.phaseId = phaseId;
        this.connections = connections;
        this.intervalSeconds = intervalSeconds;
    }

    /**
     * Derive the sidecar path from the metrics path: a trailing {@code .ndjson} is replaced by
     * {@code .interval.ndjson}; otherwise {@code .interval.ndjson} is appended.
     */
    public static String sidecarPath(String metricsPath) {
        if (metricsPath.endsWith(METRICS_SUFFIX)) {
            return metricsPath.substring(0, metricsPath.length() - METRICS_SUFFIX.length()) + INTERVAL_SUFFIX;
        }
        return metricsPath + INTERVAL_SUFFIX;
    }

    /** Open (create or append to) the sidecar file. */
    public synchronized void open() throws IOException {
        Path parent = outputPath.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /** Start the interval clock and the snapshot thread. Call when the workload loop starts. */
    public synchronized void start() {
        workloadStartMs = System.currentTimeMillis();
        windowStartMs = workloadStartMs;
        index = 0;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "interval-reporter-" + phaseId);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(() -> snapshot(false),
                intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    /** Thread-safe; called from worker virtual threads for every completed request. */
    public void record(CommandResult result) {
        WindowSlot slot = slots.get(result.commandName());
        if (slot == null) {
            slot = slots.computeIfAbsent(result.commandName(), k -> new WindowSlot());
        }
        if (result.success()) {
            slot.recorder.recordValue(Math.min(result.latencyMicros(), MAX_LATENCY_MICROS));
        } else {
            slot.errors.incrementAndGet();
        }
        slot.requests.incrementAndGet();
    }

    /**
     * Stop the snapshot thread, emit the final partial interval (if it recorded any request) and
     * close the sidecar. Call after all workers have finished.
     */
    public void stop() {
        ScheduledExecutorService s;
        synchronized (this) {
            s = scheduler;
            scheduler = null;
        }
        if (s != null) {
            s.shutdown();
            try {
                if (!s.awaitTermination(30, TimeUnit.SECONDS)) {
                    s.shutdownNow();
                }
            } catch (InterruptedException e) {
                s.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        snapshot(true);
        synchronized (this) {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    logger.warn("Failed to close interval sidecar {}: {}", outputPath, e.getMessage());
                }
                writer = null;
            }
        }
    }

    private synchronized void snapshot(boolean finalSnapshot) {
        if (writer == null) {
            return;
        }
        long now = System.currentTimeMillis();
        long start = windowStartMs;
        windowStartMs = now;

        ObjectNode metrics = objectMapper.createObjectNode();
        long totalRequests = 0;
        long totalErrors = 0;
        for (Map.Entry<String, WindowSlot> entry : slots.entrySet()) {
            WindowSlot slot = entry.getValue();
            Histogram histogram = slot.recorder.getIntervalHistogram(slot.recycle);
            slot.recycle = histogram;
            long requests = slot.requests.getAndSet(0);
            long errors = slot.errors.getAndSet(0);
            if (requests == 0 && histogram.getTotalCount() == 0) {
                continue; // no samples in this window
            }
            totalRequests += requests;
            totalErrors += errors;
            NdjsonMetricsWriter.putCommandMetrics(metrics, entry.getKey(), requests, errors, histogram);
        }

        if (finalSnapshot && totalRequests == 0) {
            return;
        }

        long durationMs = now - start;
        ObjectNode root = objectMapper.createObjectNode();
        root.put("type", "interval");
        root.put("phase_id", phaseId);
        ObjectNode interval = root.putObject("interval");
        interval.put("index", index++);
        interval.put("start_timestamp", Instant.ofEpochMilli(start).toString());
        interval.put("end_timestamp", Instant.ofEpochMilli(now).toString());
        interval.put("elapsed_s", round1((now - workloadStartMs) / 1000.0));
        interval.put("duration_s", round1(durationMs / 1000.0));
        root.put("connections", connections);
        ObjectNode totals = root.putObject("totals");
        totals.put("requests", totalRequests);
        totals.put("errors", totalErrors);
        root.put("rps", durationMs > 0 ? Math.round(totalRequests * 1000.0 / durationMs) : 0L);
        root.set("metrics", metrics);
        root.set("memory", memorySnapshot());

        try {
            writer.write(objectMapper.writeValueAsString(root));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            logger.warn("Failed to write interval record to {}: {}", outputPath, e.getMessage());
        }
    }

    private static double round1(double v) {
        return Math.round(v * 10.0) / 10.0;
    }

    /** JVM memory block; fields that cannot be read are omitted. */
    ObjectNode memorySnapshot() {
        ObjectNode memory = objectMapper.createObjectNode();
        long rss = readRssBytes();
        if (rss >= 0) {
            memory.put("rss_bytes", rss);
        }
        MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = mem.getHeapMemoryUsage();
        memory.put("heap_used_bytes", heap.getUsed());
        memory.put("heap_committed_bytes", heap.getCommitted());
        if (heap.getMax() >= 0) {
            memory.put("heap_max_bytes", heap.getMax());
        }
        memory.put("nonheap_used_bytes", mem.getNonHeapMemoryUsage().getUsed());
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if ("direct".equals(pool.getName()) && pool.getMemoryUsed() >= 0) {
                memory.put("direct_buffer_bytes", pool.getMemoryUsed());
            }
        }
        long gcCount = 0;
        long gcTime = 0;
        boolean gcCountKnown = false;
        boolean gcTimeKnown = false;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (gc.getCollectionCount() >= 0) {
                gcCount += gc.getCollectionCount();
                gcCountKnown = true;
            }
            if (gc.getCollectionTime() >= 0) {
                gcTime += gc.getCollectionTime();
                gcTimeKnown = true;
            }
        }
        if (gcCountKnown) {
            memory.put("gc_count", gcCount);
        }
        if (gcTimeKnown) {
            memory.put("gc_time_ms", gcTime);
        }
        memory.put("thread_count", ManagementFactory.getThreadMXBean().getThreadCount());
        return memory;
    }

    private static final long PAGE_SIZE = detectPageSize();

    /** Resident set size from /proc/self/statm (pages) x page size, or -1 if unavailable. */
    static long readRssBytes() {
        if (PAGE_SIZE <= 0) {
            return -1;
        }
        try {
            Path statm = Path.of("/proc/self/statm");
            if (!Files.isReadable(statm)) {
                return -1;
            }
            String[] fields = Files.readString(statm).trim().split("\\s+");
            if (fields.length < 2) {
                return -1;
            }
            return Long.parseLong(fields[1]) * PAGE_SIZE;
        } catch (Exception e) {
            return -1;
        }
    }

    private static long detectPageSize() {
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            java.lang.reflect.Field f = unsafeClass.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            Object unsafe = f.get(null);
            return ((Number) unsafeClass.getMethod("pageSize").invoke(unsafe)).longValue();
        } catch (Throwable t) {
            return -1;
        }
    }

    private static final class WindowSlot {
        final Recorder recorder = new Recorder(MAX_LATENCY_MICROS, 3);
        final AtomicLong requests = new AtomicLong();
        final AtomicLong errors = new AtomicLong();
        Histogram recycle; // only touched by the (synchronized) snapshot
    }
}
