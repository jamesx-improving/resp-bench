/*
 * Copyright 2025 the original author or authors.
 */
package io.valkey.javabenchmark.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.valkey.javabenchmark.metrics.MetricsCollector.CommandMetrics;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;

/**
 * Writes benchmark metrics to NDJSON format (Newline Delimited JSON).
 * Each phase is written as a single JSON line, allowing the orchestrator
 * to detect phase completion by watching for new lines.
 * 
 * Includes native HDR histogram encoding with base64 payload for full fidelity latency data.
 *
 * @author Ilia Kolominsky
 */
public class NdjsonMetricsWriter {
    private static final Logger logger = LoggerFactory.getLogger(NdjsonMetricsWriter.class);
    
    private final Path outputPath;
    private final ObjectMapper objectMapper;
    
    // Metadata fields
    private String commitId;
    private String driverId;
    private String primaryDriverVersion;
    private String secondaryDriverId;
    private String secondaryDriverVersion;

    public NdjsonMetricsWriter(String path) {
        this.outputPath = Path.of(path);
        this.objectMapper = new ObjectMapper();
        // No pretty printing - NDJSON requires compact single-line JSON
    }

    /**
     * Set metadata for the benchmark run.
     * 
     * @param commitId the git commit ID of the benchmark code
     * @param driverId the primary driver ID
     * @param primaryDriverVersion the primary driver version
     * @param secondaryDriverId the secondary driver ID (for spring-data-* drivers)
     * @param secondaryDriverVersion the secondary driver version
     */
    public void setMetadata(String commitId, String driverId, String primaryDriverVersion,
                           String secondaryDriverId, String secondaryDriverVersion) {
        this.commitId = commitId;
        this.driverId = driverId;
        this.primaryDriverVersion = primaryDriverVersion;
        this.secondaryDriverId = secondaryDriverId;
        this.secondaryDriverVersion = secondaryDriverVersion;
    }

    /**
     * Writes phase results as a single NDJSON line.
     */
    public void writePhaseResults(String phaseId, String status, int connections, 
                                   MetricsCollector collector) throws IOException {
        
        // Ensure parent directory exists
        Files.createDirectories(outputPath.getParent());
        
        // Build the JSON object
        ObjectNode root = buildPhaseJson(phaseId, status, connections, collector);
        
        // Write as single line (no pretty printing) + newline
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, 
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(objectMapper.writeValueAsString(root));
            writer.newLine();
        }
        
        logger.info("Wrote NDJSON metrics for phase {} to {}", phaseId, outputPath);
    }

    private ObjectNode buildPhaseJson(String phaseId, String status, int connections,
                                       MetricsCollector collector) {
        ObjectNode root = objectMapper.createObjectNode();
        
        // Metadata (commit, driver info)
        if (commitId != null || driverId != null) {
            ObjectNode metadata = root.putObject("metadata");
            if (commitId != null) {
                metadata.put("commit_id", commitId);
            }
            metadata.put("timestamp", Instant.now().toString());
            if (driverId != null) {
                metadata.put("driver_id", driverId);
            }
            if (primaryDriverVersion != null) {
                metadata.put("primary_driver_version", primaryDriverVersion);
            }
            if (secondaryDriverId != null) {
                metadata.put("secondary_driver_id", secondaryDriverId);
            }
            if (secondaryDriverVersion != null) {
                metadata.put("secondary_driver_version", secondaryDriverVersion);
            }
        }
        
        // Phase metadata
        ObjectNode phase = root.putObject("phase");
        phase.put("id", phaseId);
        phase.put("status", status);
        phase.put("start_timestamp", Instant.ofEpochMilli(collector.getStartTime()).toString());
        phase.put("finish_timestamp", Instant.ofEpochMilli(collector.getEndTime()).toString());
        phase.put("duration_ms", collector.getDurationMillis());
        phase.put("connections", connections);
        
        // Totals
        ObjectNode totals = root.putObject("totals");
        totals.put("requests", collector.getTotalRequests());
        totals.put("errors", collector.getTotalErrors());
        
        // Per-command metrics
        ObjectNode metrics = root.putObject("metrics");
        for (Map.Entry<String, CommandMetrics> entry : collector.getAllMetrics().entrySet()) {
            String cmdName = entry.getKey();
            CommandMetrics cmdMetrics = entry.getValue();
            
            putCommandMetrics(metrics, cmdName, cmdMetrics.getRequests(), cmdMetrics.getErrors(),
                    cmdMetrics.getHistogram());
        }
        
        return root;
    }

    /**
     * Writes one {@code metrics.<CMD>} entry (requests, errors, latency summary + HDR payload).
     * Shared by the phase record and the interval record so both use identical keys and encoding.
     */
    static void putCommandMetrics(ObjectNode metrics, String cmdName, long requests, long errors,
                                  Histogram histogram) {
        ObjectNode cmdNode = metrics.putObject(cmdName);
        cmdNode.put("requests", requests);
        cmdNode.put("errors", errors);

        // Latency sub-object
        ObjectNode latency = cmdNode.putObject("latency");
        latency.put("unit", "us");
        latency.put("count", histogram.getTotalCount());

        // Summary percentiles
        ObjectNode summary = latency.putObject("summary");
        summary.put("min", histogram.getMinValue());
        summary.put("p50", histogram.getValueAtPercentile(50));
        summary.put("p95", histogram.getValueAtPercentile(95));
        summary.put("p99", histogram.getValueAtPercentile(99));
        summary.put("p999", histogram.getValueAtPercentile(99.9));
        summary.put("max", histogram.getMaxValue());

        // HDR histogram with native encoding
        ObjectNode hdr = latency.putObject("hdr");
        hdr.put("format", "hdr");
        hdr.put("sigfig", 3);
        hdr.put("payload_b64", encodeHistogram(histogram));
    }

    /**
     * Encodes the histogram to a base64 string using HDR's native compressed format.
     */
    static String encodeHistogram(Histogram histogram) {
        try {
            int neededCapacity = histogram.getNeededByteBufferCapacity();
            ByteBuffer buffer = ByteBuffer.allocate(neededCapacity);
            int bytesWritten = histogram.encodeIntoCompressedByteBuffer(buffer);
            
            // Extract only the bytes that were written
            byte[] compressedBytes = new byte[bytesWritten];
            buffer.flip();
            buffer.get(compressedBytes);
            
            return Base64.getEncoder().encodeToString(compressedBytes);
        } catch (Exception e) {
            logger.warn("Failed to encode histogram: {}", e.getMessage());
            return "";
        }
    }
}