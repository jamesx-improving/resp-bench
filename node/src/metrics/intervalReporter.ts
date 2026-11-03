/**
 * Interval reporting for soak tests (docs/INTERVAL_REPORTING.md).
 *
 * While a phase with `reporting_interval_seconds > 0` runs, one `interval`
 * record per window is appended to a sidecar next to the metrics file
 * (`<metrics minus .ndjson>.interval.ndjson`), never to the metrics file itself.
 * Each record carries a per-window histogram per command (same keys and HDR
 * encoding as the phase record) plus a `process.memoryUsage()` snapshot.
 *
 * Every record is written with `appendFileSync`, so it is on disk before the
 * next one is built and a crash keeps everything written so far.
 */

import { appendFileSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

import type { MetricsCollector } from './collector.js';
import { NdjsonWriter } from './ndjsonWriter.js';

/** `x.ndjson` -> `x.interval.ndjson`; any other path gets `.interval.ndjson` appended. */
export function intervalSidecarPath(metricsPath: string): string {
  const base = metricsPath.endsWith('.ndjson')
    ? metricsPath.slice(0, -'.ndjson'.length)
    : metricsPath;
  return `${base}.interval.ndjson`;
}

/** Node runtime memory; every field is an integer byte count. */
export function memorySnapshot(): Record<string, number> {
  const m = process.memoryUsage();
  return {
    rss_bytes: m.rss,
    heap_used_bytes: m.heapUsed,
    heap_total_bytes: m.heapTotal,
    external_bytes: m.external,
    array_buffers_bytes: m.arrayBuffers,
  };
}

const round1 = (value: number): number => Math.round(value * 10) / 10;

export class IntervalReporter {
  private readonly path: string;
  private readonly phaseId: string;
  private readonly connections: number;
  private readonly intervalMs: number;
  private readonly collector: MetricsCollector;
  private readonly onError: (error: Error) => void;
  private timer: NodeJS.Timeout | null = null;
  private baseMs = 0;
  private windowStartMs = 0;
  private index = 0;
  private dirReady = false;

  constructor(options: {
    metricsPath: string;
    phaseId: string;
    connections: number;
    intervalSeconds: number;
    collector: MetricsCollector;
    onError?: (error: Error) => void;
  }) {
    this.path = intervalSidecarPath(options.metricsPath);
    this.phaseId = options.phaseId;
    this.connections = options.connections;
    this.intervalMs = options.intervalSeconds * 1000;
    this.collector = options.collector;
    this.onError = options.onError ?? (() => {});
  }

  /** Start the interval clock; call when the workload loop starts. */
  start(startMs: number = Date.now()): void {
    this.baseMs = startMs;
    this.windowStartMs = startMs;
    this.index = 0;
    this.collector.enableWindow();
    this.timer = setInterval(() => this.safeEmit(Date.now(), false), this.intervalMs);
    // Never hold the event loop open past the phase.
    this.timer.unref();
  }

  /** Stop the clock and emit the final partial window if it saw any request. */
  stop(endMs: number = Date.now()): void {
    if (this.timer === null) return;
    clearInterval(this.timer);
    this.timer = null;
    this.safeEmit(endMs, true);
  }

  /** A failed sidecar write must not crash the benchmark (it would from a timer). */
  private safeEmit(nowMs: number, final: boolean): void {
    try {
      this.emit(nowMs, final);
    } catch (error) {
      this.onError(error as Error);
    }
  }

  private emit(nowMs: number, final: boolean): void {
    const window = this.collector.takeWindow();
    if (final && window.totalRequests === 0) return;

    const durationMs = nowMs - this.windowStartMs;
    const durationS = round1(durationMs / 1000);
    const metrics: Record<string, unknown> = {};
    for (const [name, m] of window.commandMetrics) {
      if (m.requests === 0) continue;
      metrics[name] = NdjsonWriter.buildCommandRecord(m);
    }

    const record = {
      type: 'interval',
      phase_id: this.phaseId,
      interval: {
        index: this.index,
        start_timestamp: new Date(this.windowStartMs).toISOString(),
        end_timestamp: new Date(nowMs).toISOString(),
        elapsed_s: round1((nowMs - this.baseMs) / 1000),
        duration_s: durationS,
      },
      connections: this.connections,
      totals: { requests: window.totalRequests, errors: window.totalErrors },
      // round(requests / duration_s); falls back to the unrounded duration for a
      // final window shorter than 0.05s so it never divides by zero.
      rps:
        durationS > 0
          ? Math.round(window.totalRequests / durationS)
          : durationMs > 0
            ? Math.round((window.totalRequests * 1000) / durationMs)
            : 0,
      metrics,
      memory: memorySnapshot(),
    };

    this.index += 1;
    this.windowStartMs = nowMs;

    if (!this.dirReady) {
      const parent = dirname(this.path);
      if (parent && parent !== '.') mkdirSync(parent, { recursive: true });
      this.dirReady = true;
    }
    appendFileSync(this.path, `${JSON.stringify(record)}\n`, 'utf8');
  }
}
