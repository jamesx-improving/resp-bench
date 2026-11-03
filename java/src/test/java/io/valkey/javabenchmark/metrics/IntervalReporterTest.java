/*
 * Copyright 2025 the original author or authors.
 */
package io.valkey.javabenchmark.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

class IntervalReporterTest {

    @Test
    void sidecarReplacesNdjsonSuffix() {
        assertThat(IntervalReporter.sidecarPath("results/x/valkey-glide@connections=64.ndjson"))
                .isEqualTo("results/x/valkey-glide@connections=64.interval.ndjson");
    }

    @Test
    void sidecarAppendsWhenNoNdjsonSuffix() {
        assertThat(IntervalReporter.sidecarPath("results/x/metrics.json"))
                .isEqualTo("results/x/metrics.json.interval.ndjson");
        assertThat(IntervalReporter.sidecarPath("metrics"))
                .isEqualTo("metrics.interval.ndjson");
    }

    @Test
    void memoryBlockHasJvmFields() {
        IntervalReporter reporter = new IntervalReporter(Path.of("unused.ndjson"), "P", 1, 1);
        JsonNode memory = reporter.memorySnapshot();
        assertThat(memory.has("heap_used_bytes")).isTrue();
        assertThat(memory.has("heap_committed_bytes")).isTrue();
        assertThat(memory.has("nonheap_used_bytes")).isTrue();
        assertThat(memory.has("thread_count")).isTrue();
        assertThat(memory.has("gc_count")).isTrue();
        assertThat(memory.has("gc_time_ms")).isTrue();
        // rss_bytes only where /proc/self/statm is readable (Linux); never faked
        assertThat(memory.has("rss_bytes"))
                .isEqualTo(java.nio.file.Files.isReadable(Path.of("/proc/self/statm")));
        memory.fields().forEachRemaining(e -> assertThat(e.getValue().isIntegralNumber()).isTrue());
    }
}
