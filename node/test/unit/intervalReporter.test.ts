import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, it } from 'node:test';

import { ConfigError, ConfigLoader } from '../../src/config/loader.js';
import { BenchmarkEngine, type Logger } from '../../src/engine/benchmark.js';
import { MetricsCollector } from '../../src/metrics/collector.js';
import { decodeBase64 } from '../../src/metrics/hdrHistogram.js';
import {
  IntervalReporter,
  intervalSidecarPath,
  memorySnapshot,
} from '../../src/metrics/intervalReporter.js';

const silentLogger: Logger = { info: () => {}, warn: () => {}, error: () => {} };

const PHASE = {
  id: 'STEADY',
  connections: 2,
  completion: { type: 'requests', requests: 100 },
  keyspace: { keys_count: 50 },
  commands: [{ command: 'get', weight: 1.0 }],
};

function readLines(path: string): Array<Record<string, any>> {
  return readFileSync(path, 'utf8')
    .trimEnd()
    .split('\n')
    .map((line) => JSON.parse(line));
}

function tempDir(): string {
  return mkdtempSync(join(tmpdir(), 'resp-bench-node-'));
}

describe('reporting_interval_seconds config', () => {
  it('defaults to 0 (disabled)', () => {
    const phase = ConfigLoader.parsePhase(PHASE);
    assert.equal(phase.reportingIntervalSeconds, 0);
    assert.equal(phase.hasIntervalReporting(), false);
  });

  it('accepts 0 and positive integers', () => {
    assert.equal(
      ConfigLoader.parsePhase({ ...PHASE, reporting_interval_seconds: 0 }).hasIntervalReporting(),
      false,
    );
    const phase = ConfigLoader.parsePhase({ ...PHASE, reporting_interval_seconds: 30 });
    assert.equal(phase.reportingIntervalSeconds, 30);
    assert.equal(phase.hasIntervalReporting(), true);
  });

  for (const bad of [-1, 1.5, '30']) {
    it(`rejects ${JSON.stringify(bad)}`, () => {
      assert.throws(
        () => ConfigLoader.parsePhase({ ...PHASE, reporting_interval_seconds: bad }),
        (error: unknown) =>
          error instanceof ConfigError && /reporting_interval_seconds/.test(error.message),
      );
    });
  }
});

describe('intervalSidecarPath', () => {
  it('replaces a trailing .ndjson', () => {
    assert.equal(
      intervalSidecarPath('results/x/valkey-glide@connections=64.ndjson'),
      'results/x/valkey-glide@connections=64.interval.ndjson',
    );
  });

  it('appends when the path does not end in .ndjson', () => {
    assert.equal(intervalSidecarPath('out/metrics.json'), 'out/metrics.json.interval.ndjson');
    assert.equal(intervalSidecarPath('metrics'), 'metrics.interval.ndjson');
  });
});

describe('memorySnapshot', () => {
  it('reports the Node fields as integers', () => {
    const memory = memorySnapshot();
    assert.deepEqual(Object.keys(memory).sort(), [
      'array_buffers_bytes',
      'external_bytes',
      'heap_total_bytes',
      'heap_used_bytes',
      'rss_bytes',
    ]);
    for (const value of Object.values(memory)) assert.ok(Number.isInteger(value) && value >= 0);
  });
});

describe('IntervalReporter', () => {
  it('writes per-window histograms and skips an empty final window', () => {
    const metricsPath = join(tempDir(), 'm.ndjson');
    const collector = new MetricsCollector();
    const reporter = new IntervalReporter({
      metricsPath,
      phaseId: 'P',
      connections: 3,
      intervalSeconds: 3600, // never fires on its own in this test
      collector,
    });
    collector.start();
    reporter.start(1_000_000);
    for (let i = 0; i < 5; i++) {
      collector.record({ commandName: 'GET', latencyMicros: 100, success: true });
    }
    collector.record({ commandName: 'SET', latencyMicros: 0, success: false, errorMessage: 'x' });
    reporter.stop(1_002_500);

    const records = readLines(intervalSidecarPath(metricsPath));
    assert.equal(records.length, 1);
    const r = records[0]!;
    assert.equal(r['type'], 'interval');
    assert.equal(r['phase_id'], 'P');
    assert.equal(r['connections'], 3);
    assert.deepEqual(r['interval'], {
      index: 0,
      start_timestamp: new Date(1_000_000).toISOString(),
      end_timestamp: new Date(1_002_500).toISOString(),
      elapsed_s: 2.5,
      duration_s: 2.5,
    });
    assert.deepEqual(r['totals'], { requests: 6, errors: 1 });
    assert.equal(r['rps'], Math.round(6 / 2.5));
    assert.equal(r['metrics'].GET.requests, 5);
    assert.equal(r['metrics'].GET.latency.count, 5);
    assert.equal(decodeBase64(r['metrics'].GET.latency.hdr.payload_b64).totalCount, 5);
    assert.equal(r['metrics'].SET.errors, 1);
    assert.ok(Number.isInteger(r['memory'].rss_bytes));
    // Cumulative phase metrics are untouched by the window swap.
    assert.equal(collector.totalRequests, 6);
    assert.equal(collector.commandMetrics.get('GET')!.count(), 5);

    // A second stop is a no-op, and an empty final window writes nothing.
    reporter.stop();
    assert.equal(readLines(intervalSidecarPath(metricsPath)).length, 1);
  });

  it('does not create the sidecar when disabled', async () => {
    const metricsPath = join(tempDir(), 'metrics.ndjson');
    await runEngine(metricsPath, { ...PHASE, completion: { type: 'requests', requests: 20 } });
    assert.equal(readLines(metricsPath).length, 1);
    assert.equal(existsSync(intervalSidecarPath(metricsPath)), false);
  });

  it('end to end: 3s phase with a 1s interval sums to the phase total', async () => {
    const metricsPath = join(tempDir(), 'metrics.ndjson');
    await runEngine(
      metricsPath,
      {
        id: 'SOAK',
        connections: 2,
        reporting_interval_seconds: 1,
        completion: { type: 'duration', seconds: 3 },
        keyspace: { keys_count: 50 },
        commands: [
          { command: 'set', weight: 0.5, data_size_bytes: 8 },
          { command: 'get', weight: 0.5 },
        ],
      },
      // A real (timer-based) delay, so the interval timer gets to run mid-phase.
      { operation_delay_micros: 1000 },
    );

    const [phase] = readLines(metricsPath);
    assert.equal(phase!['phase'].id, 'SOAK');
    assert.equal(phase!['type'], undefined); // phase record shape unchanged

    const intervals = readLines(intervalSidecarPath(metricsPath));
    assert.ok(intervals.length >= 3, `expected >= 3 interval records, got ${intervals.length}`);
    intervals.forEach((r, i) => {
      assert.equal(r['type'], 'interval');
      assert.equal(r['phase_id'], 'SOAK');
      assert.equal(r['interval'].index, i);
    });
    for (let i = 1; i < intervals.length; i++) {
      assert.ok(intervals[i]!['interval'].elapsed_s >= intervals[i - 1]!['interval'].elapsed_s);
      assert.equal(
        intervals[i]!['interval'].start_timestamp,
        intervals[i - 1]!['interval'].end_timestamp,
      );
    }

    const sum = (pick: (r: Record<string, any>) => number) =>
      intervals.reduce((acc, r) => acc + pick(r), 0);
    assert.equal(sum((r) => r['totals'].requests), phase!['totals'].requests);
    assert.equal(sum((r) => r['totals'].errors), phase!['totals'].errors);
    for (const name of Object.keys(phase!['metrics'])) {
      assert.equal(
        sum((r) => r['metrics'][name]?.requests ?? 0),
        phase!['metrics'][name].requests,
        name,
      );
      assert.equal(
        sum((r) => r['metrics'][name]?.latency.count ?? 0),
        phase!['metrics'][name].latency.count,
        name,
      );
    }
  });
});

async function runEngine(
  metricsPath: string,
  phase: Record<string, unknown>,
  specific: Record<string, unknown> = {},
): Promise<void> {
  await new BenchmarkEngine({
    host: 'localhost',
    port: 6379,
    driverConfig: ConfigLoader.parseDriverConfig({
      driver_id: 'recording',
      mode: 'standalone',
      specific_driver_config: specific,
    }),
    workloadConfig: ConfigLoader.parseWorkloadConfig({
      benchmark_profile: { name: 'interval test' },
      phases: [phase],
    }),
    metricsPath,
    logger: silentLogger,
  }).run();
}
