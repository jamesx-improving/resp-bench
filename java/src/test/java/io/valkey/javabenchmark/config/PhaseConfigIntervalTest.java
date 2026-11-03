/*
 * Copyright 2025 the original author or authors.
 */
package io.valkey.javabenchmark.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class PhaseConfigIntervalTest {

    private static String workload(String intervalField) {
        return """
            {
                "benchmark_profile": {"name": "IntervalCfg"},
                "phases": [{
                    "id": "P1",
                    "connections": 1,
                    %s
                    "commands": [{"command": "get", "weight": 1.0}],
                    "keyspace": {"keys_count": 10, "key_size_bytes": 8, "generation_alg": "sequential_int"},
                    "completion": {"type": "requests", "requests": 10}
                }]
            }
            """.formatted(intervalField);
    }

    @Test
    void absentMeansDisabled() throws Exception {
        PhaseConfig phase = ConfigLoader.parseWorkloadConfig(workload("")).getPhases().get(0);
        assertThat(phase.getReportingIntervalSeconds()).isZero();
        assertThat(phase.hasReportingInterval()).isFalse();
    }

    @Test
    void zeroMeansDisabled() throws Exception {
        PhaseConfig phase = ConfigLoader.parseWorkloadConfig(
                workload("\"reporting_interval_seconds\": 0,")).getPhases().get(0);
        assertThat(phase.getReportingIntervalSeconds()).isZero();
        assertThat(phase.hasReportingInterval()).isFalse();
    }

    @Test
    void positiveIntegerIsParsed() throws Exception {
        PhaseConfig phase = ConfigLoader.parseWorkloadConfig(
                workload("\"reporting_interval_seconds\": 30,")).getPhases().get(0);
        assertThat(phase.getReportingIntervalSeconds()).isEqualTo(30);
        assertThat(phase.hasReportingInterval()).isTrue();
    }

    @Test
    void negativeIsRejected() {
        assertThatThrownBy(() -> ConfigLoader.parseWorkloadConfig(
                workload("\"reporting_interval_seconds\": -5,")))
                .isInstanceOf(ConfigLoader.ConfigurationException.class)
                .hasMessageContaining("reporting_interval_seconds");
    }

    @Test
    void nonIntegerIsRejected() {
        assertThatThrownBy(() -> ConfigLoader.parseWorkloadConfig(
                workload("\"reporting_interval_seconds\": 1.5,")))
                .isInstanceOf(ConfigLoader.ConfigurationException.class)
                .hasMessageContaining("reporting_interval_seconds");
        assertThatThrownBy(() -> ConfigLoader.parseWorkloadConfig(
                workload("\"reporting_interval_seconds\": \"30\",")))
                .isInstanceOf(ConfigLoader.ConfigurationException.class)
                .hasMessageContaining("reporting_interval_seconds");
    }
}
