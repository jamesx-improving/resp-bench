/**
 * Writes benchmark metrics as NDJSON (newline-delimited JSON), one line per
 * phase, so the orchestrator can detect phase completion by watching for new
 * lines.
 *
 * The record shape is byte-for-byte the contract in docs/ARCHITECTURE.md
 * "Metrics Output Format" plus the `metadata` block the Java writer emits
 * (NdjsonMetricsWriter.java:92-111). The matrix runner treats "exited 0 but wrote
 * no record" as a cell failure (run_benchmark_matrix.py:1063-1069), so appending
 * really must happen.
 */

import { appendFileSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

import type { CommandMetrics, MetricsCollector } from './collector.js';
import { encodeBase64, SIGNIFICANT_FIGURES } from './hdrHistogram.js';

export interface Metadata {
  commitId?: string | null;
  driverId?: string | null;
  primaryDriverVersion?: string | null;
  secondaryDriverId?: string | null;
  secondaryDriverVersion?: string | null;
}

export class NdjsonWriter {
  private readonly outputPath: string;
  private metadata: Metadata = {};

  constructor(outputPath: string) {
    this.outputPath = outputPath;
  }

  setMetadata(metadata: Metadata): void {
    this.metadata = metadata;
  }

  writePhaseResults(options: {
    phaseId: string;
    status: string;
    connections: number;
    collector: MetricsCollector;
  }): void {
    const record = this.buildPhaseRecord(options);
    const parent = dirname(this.outputPath);
    if (parent && parent !== '.') mkdirSync(parent, { recursive: true });
    appendFileSync(this.outputPath, `${JSON.stringify(record)}\n`, 'utf8');
  }

  private buildPhaseRecord(options: {
    phaseId: string;
    status: string;
    connections: number;
    collector: MetricsCollector;
  }): Record<string, unknown> {
    const { phaseId, status, connections, collector } = options;
    const record: Record<string, unknown> = {};

    const { commitId, driverId, primaryDriverVersion, secondaryDriverId, secondaryDriverVersion } =
      this.metadata;
    if (commitId != null || driverId != null) {
      const metadata: Record<string, unknown> = {};
      if (commitId != null) metadata['commit_id'] = commitId;
      metadata['timestamp'] = new Date().toISOString();
      if (driverId != null) metadata['driver_id'] = driverId;
      if (primaryDriverVersion != null) metadata['primary_driver_version'] = primaryDriverVersion;
      if (secondaryDriverId != null) metadata['secondary_driver_id'] = secondaryDriverId;
      if (secondaryDriverVersion != null) {
        metadata['secondary_driver_version'] = secondaryDriverVersion;
      }
      record['metadata'] = metadata;
    }

    record['phase'] = {
      id: phaseId,
      status,
      start_timestamp: new Date(collector.startTime()).toISOString(),
      finish_timestamp: new Date(collector.endTime()).toISOString(),
      duration_ms: collector.durationMillis(),
      connections,
    };

    record['totals'] = {
      requests: collector.totalRequests,
      errors: collector.totalErrors,
    };

    const metrics: Record<string, unknown> = {};
    for (const [commandName, commandMetrics] of collector.commandMetrics) {
      metrics[commandName] = NdjsonWriter.buildCommandRecord(commandMetrics);
    }
    record['metrics'] = metrics;

    return record;
  }

  /** Shared with the interval sidecar so both use identical keys and encoding. */
  static buildCommandRecord(m: CommandMetrics): Record<string, unknown> {
    return {
      requests: m.requests,
      errors: m.errors,
      latency: {
        unit: 'us',
        count: m.count(),
        summary: {
          min: m.min(),
          p50: m.percentile(50),
          p95: m.percentile(95),
          p99: m.percentile(99),
          p999: m.percentile(99.9),
          max: m.max(),
        },
        hdr: {
          format: 'hdr',
          sigfig: SIGNIFICANT_FIGURES,
          // Already base64 -- do NOT encode again (see metrics/hdrHistogram.ts).
          payload_b64: encodeBase64(m.histogram),
        },
      },
    };
  }
}
