/*
 * Copyright 2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.valkey.javabenchmark.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;

import java.util.List;

/**
 * Configuration model for a benchmark phase.
 *
 * @author Ilia Kolominsky
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PhaseConfig {

    @JsonProperty("id")
    private String id;

    @JsonProperty("description")
    private String description;

    @JsonProperty("connections")
    private int connections;

    @JsonProperty("cps_limit")
    private int cpsLimit = -1;

    @JsonProperty("rps_limit")
    private int rpsLimit = -1;

    @JsonProperty("pipeline_depth")
    private int pipelineDepth = 1;

    @JsonProperty("warmup_requests")
    private int warmupRequests = 1;

    @JsonProperty("completion")
    private CompletionConfig completion;

    @JsonProperty("keyspace")
    private KeyspaceConfig keyspace;

    @JsonProperty("commands")
    private List<CommandConfig> commands;

    /**
     * Raw {@code reporting_interval_seconds} value. Kept as a JsonNode so that non-integer
     * values (e.g. 1.5, "30") are rejected by {@link #validate()} instead of being silently coerced.
     */
    @JsonProperty("reporting_interval_seconds")
    private JsonNode reportingIntervalSeconds;

    // Getters and Setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getConnections() {
        return connections;
    }

    public void setConnections(int connections) {
        this.connections = connections;
    }

    public int getCpsLimit() {
        return cpsLimit;
    }

    public void setCpsLimit(int cpsLimit) {
        this.cpsLimit = cpsLimit;
    }

    public int getRpsLimit() {
        return rpsLimit;
    }

    public void setRpsLimit(int rpsLimit) {
        this.rpsLimit = rpsLimit;
    }

    public int getPipelineDepth() {
        return pipelineDepth;
    }

    public void setPipelineDepth(int pipelineDepth) {
        this.pipelineDepth = pipelineDepth;
    }

    public int getWarmupRequests() {
        return warmupRequests;
    }

    public void setWarmupRequests(int warmupRequests) {
        this.warmupRequests = warmupRequests;
    }

    public CompletionConfig getCompletion() {
        return completion;
    }

    public void setCompletion(CompletionConfig completion) {
        this.completion = completion;
    }

    public KeyspaceConfig getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(KeyspaceConfig keyspace) {
        this.keyspace = keyspace;
    }

    public List<CommandConfig> getCommands() {
        return commands;
    }

    public void setCommands(List<CommandConfig> commands) {
        this.commands = commands;
    }

    /**
     * Interval reporting period in seconds; 0 when absent (disabled).
     * Only meaningful after {@link #validate()} has succeeded.
     */
    @JsonIgnore
    public int getReportingIntervalSeconds() {
        if (reportingIntervalSeconds == null || reportingIntervalSeconds.isNull()) {
            return 0;
        }
        return reportingIntervalSeconds.asInt(0);
    }

    @JsonIgnore
    public void setReportingIntervalSeconds(int seconds) {
        this.reportingIntervalSeconds = IntNode.valueOf(seconds);
    }

    // Convenience methods

    /**
     * Check if interval reporting is enabled.
     *
     * @return true if reporting_interval_seconds > 0
     */
    public boolean hasReportingInterval() {
        return getReportingIntervalSeconds() > 0;
    }

    /**
     * Check if connection rate limiting is enabled.
     * 
     * @return true if cps_limit > 0
     */
    public boolean hasCpsLimit() {
        return cpsLimit > 0;
    }

    /**
     * Check if request rate limiting is enabled.
     * 
     * @return true if rps_limit > 0
     */
    public boolean hasRpsLimit() {
        return rpsLimit > 0;
    }

    /**
     * Validate the phase configuration.
     * 
     * @throws IllegalArgumentException if configuration is invalid
     */
    public void validate() {
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("Phase id is required");
        }
        if (connections <= 0) {
            throw new IllegalArgumentException("Phase connections must be positive");
        }
        if (completion == null) {
            throw new IllegalArgumentException("Phase completion config is required");
        }
        completion.validate();
        if (keyspace == null) {
            throw new IllegalArgumentException("Phase keyspace config is required");
        }
        keyspace.validate();
        if (commands == null || commands.isEmpty()) {
            throw new IllegalArgumentException("Phase must have at least one command");
        }
        
        // Validate command weights sum to 1
        double totalWeight = commands.stream()
                .mapToDouble(CommandConfig::getWeight)
                .sum();
        if (Math.abs(totalWeight - 1.0) > 0.001) {
            throw new IllegalArgumentException(
                    "Command weights must sum to 1.0, got " + totalWeight);
        }
        
        for (CommandConfig cmd : commands) {
            cmd.validate();
        }

        if (reportingIntervalSeconds != null && !reportingIntervalSeconds.isNull()) {
            if (!reportingIntervalSeconds.isIntegralNumber() || !reportingIntervalSeconds.canConvertToInt()) {
                throw new IllegalArgumentException(
                        "reporting_interval_seconds must be a non-negative integer, got "
                                + reportingIntervalSeconds);
            }
            if (reportingIntervalSeconds.asInt() < 0) {
                throw new IllegalArgumentException(
                        "reporting_interval_seconds must be a non-negative integer, got "
                                + reportingIntervalSeconds.asInt());
            }
        }
    }

    @Override
    public String toString() {
        return "PhaseConfig{" +
                "id='" + id + '\'' +
                ", connections=" + connections +
                ", pipelineDepth=" + pipelineDepth +
                ", warmupRequests=" + warmupRequests +
                ", rpsLimit=" + rpsLimit +
                ", completion=" + completion +
                ", commandsCount=" + (commands != null ? commands.size() : 0) +
                '}';
    }
}