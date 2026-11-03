/*
 * Copyright 2025 the original author or authors.
 */
package io.valkey.javabenchmark.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.valkey.javabenchmark.client.impl.RecordingBenchmarkClient;
import io.valkey.javabenchmark.config.*;
import io.valkey.javabenchmark.engine.BenchmarkEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * End-to-end interval reporting test using the recording driver (no server).
 */
class IntervalReportingTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    @BeforeEach
    void clearInstances() {
        RecordingBenchmarkClient.clearInstances();
    }

    private static String workload(String intervalField) {
        return """
            {
                "benchmark_profile": {"name": "IntervalTest"},
                "phases": [{
                    "id": "STEADY",
                    "connections": 2,
                    "rps_limit": 400,
                    %s
                    "commands": [
                        {"command": "set", "weight": 0.5, "data_size_bytes": 16},
                        {"command": "get", "weight": 0.5}
                    ],
                    "keyspace": {"key_prefix": "iv:", "keys_count": 100, "key_size_bytes": 16,
                                 "generation_alg": "sequential_int"},
                    "completion": {"type": "duration", "seconds": 3}
                }]
            }
            """.formatted(intervalField);
    }

    @Test
    void intervalRecordsSumToPhaseTotals() throws Exception {
        DriverConfig driver = ConfigLoader.parseDriverConfig("{\"driver_id\": \"recording\", \"mode\": \"standalone\"}");
        WorkloadConfig workload = ConfigLoader.parseWorkloadConfig(
                workload("\"reporting_interval_seconds\": 1,"));

        Path metricsFile = tempDir.resolve("run.ndjson");
        new BenchmarkEngine("localhost", 6379, driver, workload, metricsFile.toString()).run();

        // Phase record unchanged: exactly one line in the metrics file, no interval records
        List<String> phaseLines = Files.readAllLines(metricsFile);
        assertThat(phaseLines).hasSize(1);
        JsonNode phase = objectMapper.readTree(phaseLines.get(0));
        assertThat(phase.has("type")).isFalse();
        long phaseRequests = phase.at("/totals/requests").asLong();
        long phaseErrors = phase.at("/totals/errors").asLong();
        assertThat(phaseRequests).isPositive();

        Path sidecar = tempDir.resolve("run.interval.ndjson");
        assertThat(sidecar).exists();
        List<JsonNode> records = new ArrayList<>();
        for (String line : Files.readAllLines(sidecar)) {
            records.add(objectMapper.readTree(line));
        }
        // ~3 full 1s windows plus possibly a final partial
        assertThat(records.size()).isBetween(2, 5);

        long sumRequests = 0;
        long sumErrors = 0;
        for (int i = 0; i < records.size(); i++) {
            JsonNode r = records.get(i);
            assertThat(r.get("type").asText()).isEqualTo("interval");
            assertThat(r.get("phase_id").asText()).isEqualTo("STEADY");
            assertThat(r.at("/interval/index").asInt()).isEqualTo(i);
            assertThat(r.at("/interval/start_timestamp").isTextual()).isTrue();
            assertThat(r.at("/interval/end_timestamp").isTextual()).isTrue();
            assertThat(r.at("/interval/duration_s").asDouble()).isLessThanOrEqualTo(1.5);
            assertThat(r.get("connections").asInt()).isEqualTo(2);
            assertThat(r.has("rps")).isTrue();
            assertThat(r.at("/memory/heap_used_bytes").isIntegralNumber()).isTrue();
            if (i > 0) {
                assertThat(r.at("/interval/elapsed_s").asDouble())
                        .isGreaterThan(records.get(i - 1).at("/interval/elapsed_s").asDouble());
            }

            long cmdRequests = 0;
            var it = r.get("metrics").fields();
            while (it.hasNext()) {
                var e = it.next();
                assertThat(e.getKey()).isIn("SET", "GET");
                JsonNode latency = e.getValue().get("latency");
                assertThat(latency.get("unit").asText()).isEqualTo("us");
                assertThat(latency.at("/hdr/format").asText()).isEqualTo("hdr");
                assertThat(latency.at("/hdr/payload_b64").asText()).isNotEmpty();
                assertThat(latency.at("/summary/p99").isIntegralNumber()).isTrue();
                cmdRequests += e.getValue().get("requests").asLong();
            }
            assertThat(cmdRequests).isEqualTo(r.at("/totals/requests").asLong());
            sumRequests += r.at("/totals/requests").asLong();
            sumErrors += r.at("/totals/errors").asLong();
        }
        assertThat(sumRequests).isEqualTo(phaseRequests);
        assertThat(sumErrors).isEqualTo(phaseErrors);
    }

    @Test
    void disabledWritesNoSidecar() throws Exception {
        DriverConfig driver = ConfigLoader.parseDriverConfig("{\"driver_id\": \"recording\", \"mode\": \"standalone\"}");
        WorkloadConfig workload = ConfigLoader.parseWorkloadConfig(
                workload("").replace("\"seconds\": 3", "\"seconds\": 1"));

        Path metricsFile = tempDir.resolve("off.ndjson");
        new BenchmarkEngine("localhost", 6379, driver, workload, metricsFile.toString()).run();

        assertThat(metricsFile).exists();
        assertThat(tempDir.resolve("off.interval.ndjson")).doesNotExist();
    }
}
