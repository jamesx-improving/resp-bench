/*
 * Copyright 2025 the original author or authors.
 */
package io.valkey.javabenchmark.metrics;

import io.valkey.javabenchmark.command.Command.CommandResult;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SynchronizedHistogram;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects metrics for benchmark operations using HdrHistogram.
 *
 * @author Ilia Kolominsky
 */
public class MetricsCollector {
    private final Map<String, CommandMetrics> commandMetrics = new ConcurrentHashMap<>();
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private volatile long startTime;
    private volatile long endTime;
    /** Optional per-window sink for interval reporting; null when disabled. */
    private volatile IntervalReporter intervalReporter;

    public void start() {
        startTime = System.currentTimeMillis();
    }

    public void stop() {
        endTime = System.currentTimeMillis();
    }

    public void record(CommandResult result) {
        totalRequests.incrementAndGet();
        if (!result.success()) {
            totalErrors.incrementAndGet();
        }
        
        commandMetrics.computeIfAbsent(result.commandName(), CommandMetrics::new)
                .record(result);

        IntervalReporter reporter = intervalReporter;
        if (reporter != null) {
            reporter.record(result);
        }
    }

    /**
     * Attach (or detach with null) an interval reporter that receives every recorded result
     * in addition to the cumulative phase metrics.
     */
    public void setIntervalReporter(IntervalReporter reporter) {
        this.intervalReporter = reporter;
    }

    public long getTotalRequests() { return totalRequests.get(); }
    public long getTotalErrors() { return totalErrors.get(); }
    public long getStartTime() { return startTime; }
    public long getEndTime() { return endTime; }
    public long getDurationMillis() { return endTime - startTime; }

    public CommandMetrics getMetrics(String commandName) {
        return commandMetrics.get(commandName);
    }

    public Map<String, CommandMetrics> getAllMetrics() {
        return commandMetrics;
    }

    public void reset() {
        commandMetrics.clear();
        totalRequests.set(0);
        totalErrors.set(0);
        startTime = 0;
        endTime = 0;
    }

    /**
     * Metrics for a specific command.
     * Uses SynchronizedHistogram for thread-safe concurrent access from async completion handlers.
     */
    public static class CommandMetrics {
        private final String commandName;
        private final SynchronizedHistogram histogram;
        private final AtomicLong requests = new AtomicLong(0);
        private final AtomicLong errors = new AtomicLong(0);

        public CommandMetrics(String commandName) {
            this.commandName = commandName;
            // Track latencies up to 10 minutes with 3 significant digits
            // Use SynchronizedHistogram for thread-safe concurrent access
            this.histogram = new SynchronizedHistogram(600_000_000L, 3);
        }

        public void record(CommandResult result) {
            requests.incrementAndGet();
            if (result.success()) {
                histogram.recordValue(Math.min(result.latencyMicros(), 600_000_000L));
            } else {
                errors.incrementAndGet();
            }
        }

        public String getCommandName() { return commandName; }
        public long getRequests() { return requests.get(); }
        public long getErrors() { return errors.get(); }
        public Histogram getHistogram() { return histogram; }

        public long getMin() { return histogram.getMinValue(); }
        public long getMax() { return histogram.getMaxValue(); }
        public double getMean() { return histogram.getMean(); }
        public long getP50() { return histogram.getValueAtPercentile(50); }
        public long getP90() { return histogram.getValueAtPercentile(90); }
        public long getP95() { return histogram.getValueAtPercentile(95); }
        public long getP99() { return histogram.getValueAtPercentile(99); }
        public long getP999() { return histogram.getValueAtPercentile(99.9); }
    }
}