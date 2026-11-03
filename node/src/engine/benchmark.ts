/**
 * Benchmark engine.
 *
 * Concurrency model: a single event loop with **one client per connection** (the
 * `client == connection` invariant every engine holds) and **one worker per
 * connection**, all started together via `Promise.all`. Node is single-threaded
 * with async I/O, so this is the faithful analogue of Java's
 * virtual-thread-per-client design -- an awaited command parks the worker, not
 * the loop, so the other connections keep making progress.
 *
 * Two loops, mirroring Java (BenchmarkEngine.java:353-452):
 * - `pipeline_depth <= 1`: issue one command, await it, record, repeat.
 * - `pipeline_depth > 1`: keep up to `pipelineDepth` requests in flight per
 *   connection, awaiting whichever settles first and immediately refilling.
 *
 * The request budget is **shared across all workers**, claimed one request at a
 * time, exactly as Java does with its per-phase `AtomicLong`
 * (BenchmarkEngine.java:249, 363-367). It is deliberately not pre-divided per
 * worker: with a shared budget a slow connection cannot cap the run -- faster
 * workers absorb the slack and the phase ends when the total budget is spent.
 * Pre-splitting would bound wall-clock by the slowest connection and change the
 * per-connection distribution, which is a real cross-engine comparability gap.
 */

import type { BenchmarkClient } from '../client/benchmarkClient.js';
import { BenchmarkClientFactory } from '../client/factory.js';
import type { Command, CommandResult } from '../command/command.js';
import { CommandFactory } from '../command/factory.js';
import type { DriverConfig } from '../config/driverConfig.js';
import type { PhaseConfig } from '../config/phaseConfig.js';
import type { WorkloadConfig } from '../config/workloadConfig.js';
import { MetricsCollector } from '../metrics/collector.js';
import { IntervalReporter } from '../metrics/intervalReporter.js';
import { NdjsonWriter } from '../metrics/ndjsonWriter.js';
import { CommandSelector } from './commandSelector.js';
import { Counter, KeyGenerator } from './keyGenerator.js';
import { RateLimiter } from './rateLimiter.js';

const PROGRESS_LOG_INTERVAL_MS = 10_000;
const CONNECTION_LOG_INTERVAL = 50;

export interface Logger {
  info(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}

export const consoleLogger: Logger = {
  info: (message) => console.log(`${new Date().toISOString()} INFO  ${message}`),
  warn: (message) => console.warn(`${new Date().toISOString()} WARN  ${message}`),
  error: (message) => console.error(`${new Date().toISOString()} ERROR ${message}`),
};

/**
 * A shared, monotonically-drained request budget for one phase.
 *
 * `claim()` is atomic without a lock: it reads and writes with no `await` in
 * between, so concurrent workers on the single event-loop thread can never
 * interleave inside it.
 */
class RequestBudget {
  private remaining: number;

  constructor(total: number) {
    this.remaining = total;
  }

  claim(): boolean {
    if (this.remaining <= 0) return false;
    this.remaining -= 1;
    return true;
  }
}

export interface BenchmarkEngineOptions {
  host: string;
  port: number;
  driverConfig: DriverConfig;
  workloadConfig: WorkloadConfig;
  metricsPath: string;
  commitId?: string | null;
  logger?: Logger;
}

export class BenchmarkEngine {
  private readonly host: string;
  private readonly port: number;
  private readonly driverConfig: DriverConfig;
  private readonly workloadConfig: WorkloadConfig;
  private readonly writer: NdjsonWriter;
  private readonly metricsPath: string;
  private readonly commitId: string | null;
  private readonly log: Logger;

  constructor(options: BenchmarkEngineOptions) {
    this.host = options.host;
    this.port = options.port;
    this.driverConfig = options.driverConfig;
    this.workloadConfig = options.workloadConfig;
    this.writer = new NdjsonWriter(options.metricsPath);
    this.metricsPath = options.metricsPath;
    this.commitId = options.commitId ?? null;
    this.log = options.logger ?? consoleLogger;
  }

  async run(): Promise<void> {
    this.log.info(`Starting benchmark: ${this.workloadConfig.name()}`);
    this.log.info(`Driver: ${this.driverConfig.driverId}, Server mode: ${this.driverConfig.mode}`);
    this.log.info('Concurrency: event-loop task-per-connection (one client per connection)');
    this.log.info(`Server: ${this.host}:${this.port}`);

    await this.setupMetadata();

    for (const phase of this.workloadConfig.phases) {
      await this.executePhase(phase);
    }

    this.log.info('Benchmark completed');
  }

  /** Best-effort: a version lookup failure must not fail the benchmark. */
  private async setupMetadata(): Promise<void> {
    try {
      const sample = await BenchmarkClientFactory.createAndConnect(
        this.host,
        this.port,
        this.driverConfig,
      );
      const version = sample.driverVersion();
      this.writer.setMetadata({
        commitId: this.commitId,
        driverId: this.driverConfig.driverId,
        primaryDriverVersion: version,
        secondaryDriverId: this.driverConfig.secondaryDriverId(),
        secondaryDriverVersion: sample.secondaryDriverVersion?.() ?? null,
      });
      this.log.info(
        `Metadata: commit=${this.commitId ?? 'N/A'}, driver=${this.driverConfig.driverId}, version=${version}`,
      );
      await sample.close();
    } catch (error) {
      this.log.warn(`Failed to get driver version for metadata: ${(error as Error).message}`);
      this.writer.setMetadata({
        commitId: this.commitId,
        driverId: this.driverConfig.driverId,
        primaryDriverVersion: 'unknown',
        secondaryDriverId: this.driverConfig.secondaryDriverId(),
        secondaryDriverVersion: null,
      });
    }
  }

  private async executePhase(phase: PhaseConfig): Promise<void> {
    this.log.info(`=== Starting phase: ${phase.id} (${phase.description ?? ''}) ===`);

    const collector = new MetricsCollector();
    const clients = await this.createClients(phase);
    const commands = CommandFactory.createAll(phase.commands);

    let status: string;
    try {
      if (phase.warmupRequests > 0) await this.warmup(clients, phase.warmupRequests);

      // Created *after* warmup, deliberately. The limiter starts its clock at
      // construction, so building it earlier would bank the whole warmup
      // duration as credit and release a burst of
      // (warmup_duration / interval) requests the moment the workload starts --
      // defeating the evenly-spaced, no-burst property the limiter exists for.
      const rateLimiter = phase.hasRpsLimit() ? RateLimiter.create(phase.rpsLimit) : null;

      const intervals = phase.hasIntervalReporting()
        ? new IntervalReporter({
            metricsPath: this.metricsPath,
            phaseId: phase.id,
            connections: phase.connections,
            intervalSeconds: phase.reportingIntervalSeconds,
            collector,
            onError: (error) => this.log.warn(`Interval report write failed: ${error.message}`),
          })
        : null;

      collector.start();
      intervals?.start(collector.startTime());
      try {
        status = await this.runWorkload(phase, clients, commands, rateLimiter, collector);
      } finally {
        collector.stop();
        intervals?.stop(collector.endTime());
      }
    } finally {
      await this.closeClients(clients);
    }

    this.writer.writePhaseResults({
      phaseId: phase.id,
      status,
      connections: phase.connections,
      collector,
    });
    this.logPhaseSummary(phase, collector, status);
  }

  private async createClients(phase: PhaseConfig): Promise<BenchmarkClient[]> {
    this.log.info(`Creating ${phase.connections} connections...`);
    const cpsLimiter = phase.hasCpsLimit() ? RateLimiter.create(phase.cpsLimit) : null;

    const clients: BenchmarkClient[] = [];
    for (let i = 0; i < phase.connections; i++) {
      if (cpsLimiter !== null) await cpsLimiter.acquire();
      clients.push(
        await BenchmarkClientFactory.createAndConnect(this.host, this.port, this.driverConfig),
      );
      if ((i + 1) % CONNECTION_LOG_INTERVAL === 0) {
        this.log.info(`Created ${i + 1}/${phase.connections} connections`);
      }
    }
    this.log.info(`All ${clients.length} connections established`);
    return clients;
  }

  /**
   * Send warmup PINGs on every client, failing fast if any of them errors.
   *
   * A dead or misconfigured server would otherwise produce a whole phase of
   * nothing but errors, which is far harder to diagnose than an upfront throw.
   *
   * Uses `allSettled`, not `all`: `all` rejects on the first failure while the
   * remaining warmup loops keep running unawaited, so `executePhase`'s `finally`
   * would close the clients underneath them. Settling every loop first means a
   * warmup failure leaves nothing in flight.
   */
  private async warmup(clients: BenchmarkClient[], warmupRequests: number): Promise<void> {
    this.log.info(`Warmup: ${warmupRequests} PING(s) per client...`);
    // Warmup mode lets the recording driver suppress simulated errors, so an
    // error_rate workload is not aborted by the very errors it is measuring.
    for (const client of clients) client.setWarmupMode?.(true);
    let outcomes: PromiseSettledResult<void>[];
    try {
      outcomes = await Promise.allSettled(
        clients.map(async (client) => {
          for (let i = 0; i < warmupRequests; i++) {
            const result = await client.ping();
            if (result.error !== undefined) {
              throw new Error(`Warmup PING failed: ${result.error.message}`);
            }
          }
        }),
      );
    } finally {
      for (const client of clients) client.setWarmupMode?.(false);
    }

    const failure = outcomes.find((o): o is PromiseRejectedResult => o.status === 'rejected');
    if (failure !== undefined) {
      throw failure.reason instanceof Error ? failure.reason : new Error(String(failure.reason));
    }
    this.log.info('Warmup completed');
  }

  private async runWorkload(
    phase: PhaseConfig,
    clients: BenchmarkClient[],
    commands: Command[],
    rateLimiter: RateLimiter | null,
    collector: MetricsCollector,
  ): Promise<string> {
    const { completion } = phase;
    const pipelineDepth = phase.effectivePipelineDepth();
    const seedBase = phase.keyspace.seedValue();
    // Shared across workers for sequential_int, so they collectively emit
    // 0, 1, 2, ... exactly as the Java reference does.
    const sharedCounter = new Counter();

    const budget = completion.isRequestBased() ? new RequestBudget(completion.totalRequests()) : null;
    const deadlineMs = completion.isDurationBased()
      ? Date.now() + completion.durationSeconds() * 1000
      : null;

    const keepGoing = (): boolean => {
      if (deadlineMs !== null && Date.now() >= deadlineMs) return false;
      if (budget !== null) return budget.claim();
      return true;
    };

    this.log.info(
      `Starting ${clients.length} workers (pipeline_depth=${pipelineDepth})...`,
    );

    const progressTimer = setInterval(() => {
      this.logProgress(collector, completion.isRequestBased() ? completion.totalRequests() : null);
    }, PROGRESS_LOG_INTERVAL_MS);
    // Do not let the interval hold the event loop open past the phase.
    progressTimer.unref();

    try {
      await Promise.all(
        clients.map((client, index) => {
          const keyGen = KeyGenerator.createWithSeed(phase.keyspace, seedBase + index, sharedCounter);
          const selector = new CommandSelector(commands);
          return pipelineDepth <= 1
            ? this.runSyncLoop(client, selector, keyGen, rateLimiter, collector, keepGoing)
            : this.runPipelinedLoop(
                client,
                selector,
                keyGen,
                rateLimiter,
                collector,
                keepGoing,
                pipelineDepth,
              );
        }),
      );
      this.log.info(`All operations completed (${collector.totalRequests} total requests)`);
      return 'COMPLETED';
    } catch (error) {
      this.log.error(`Error during workload execution: ${(error as Error).message}`);
      return 'ERROR';
    } finally {
      clearInterval(progressTimer);
    }
  }

  /** One in-flight request per connection (pipeline_depth <= 1). */
  private async runSyncLoop(
    client: BenchmarkClient,
    selector: CommandSelector,
    keyGen: KeyGenerator,
    rateLimiter: RateLimiter | null,
    collector: MetricsCollector,
    keepGoing: () => boolean,
  ): Promise<void> {
    while (keepGoing()) {
      if (rateLimiter !== null) await rateLimiter.acquire();
      collector.record(await this.issue(client, selector, keyGen));
    }
  }

  /**
   * Up to `pipelineDepth` in-flight requests per connection.
   *
   * Each slot runs its own claim/issue/record cycle, so a settled request is
   * replaced immediately rather than waiting on a whole batch -- the same
   * "refill as they land" behaviour as Java's `anyOf` loop, expressed as N
   * independent slot loops sharing the connection.
   */
  private async runPipelinedLoop(
    client: BenchmarkClient,
    selector: CommandSelector,
    keyGen: KeyGenerator,
    rateLimiter: RateLimiter | null,
    collector: MetricsCollector,
    keepGoing: () => boolean,
    pipelineDepth: number,
  ): Promise<void> {
    const slot = async (): Promise<void> => {
      while (keepGoing()) {
        if (rateLimiter !== null) await rateLimiter.acquire();
        collector.record(await this.issue(client, selector, keyGen));
      }
    };
    await Promise.all(Array.from({ length: pipelineDepth }, slot));
  }

  /**
   * Select and run one command.
   *
   * Drivers already convert a rejection into a failed TimedResult, so a throw
   * here means an engine-level bug rather than a server error; record it as a
   * failed request and keep the phase running.
   */
  private async issue(
    client: BenchmarkClient,
    selector: CommandSelector,
    keyGen: KeyGenerator,
  ): Promise<CommandResult> {
    const command = selector.select();
    // Only advance the key sequence for commands that consume a key, so PING
    // does not silently shift the sequence other engines produce.
    const key = command.usesKey ? keyGen.nextKey() : '';
    try {
      return await command.execute(client, key);
    } catch (error) {
      return {
        commandName: command.name,
        latencyMicros: 0,
        success: false,
        errorMessage: (error as Error).message,
      };
    }
  }

  private async closeClients(clients: BenchmarkClient[]): Promise<void> {
    this.log.info(`Closing ${clients.length} connections...`);
    for (const client of clients) {
      try {
        await client.close();
      } catch (error) {
        this.log.warn(`Error closing client: ${(error as Error).message}`);
      }
    }
  }

  private logProgress(collector: MetricsCollector, target: number | null): void {
    const elapsedMs = Date.now() - collector.startTime();
    if (elapsedMs <= 0) return;
    const current = collector.totalRequests;
    const rate = Math.round((current * 1000) / elapsedMs);
    if (target !== null) {
      const percent = ((current * 100) / target).toFixed(1);
      this.log.info(`Progress: ${current}/${target} requests (${percent}%) - ${rate} req/s`);
    } else {
      this.log.info(`Progress: ${current} requests - ${rate} req/s`);
    }
  }

  private logPhaseSummary(phase: PhaseConfig, collector: MetricsCollector, status: string): void {
    const durationSeconds = collector.durationMillis() / 1000;
    const rps = durationSeconds > 0 ? Math.round(collector.totalRequests / durationSeconds) : 0;
    this.log.info(`=== Phase ${phase.id} completed: ${status} ===`);
    this.log.info(
      `  Duration: ${durationSeconds.toFixed(1)}s | Requests: ${collector.totalRequests} | ` +
        `Errors: ${collector.totalErrors} | RPS: ${rps}`,
    );
    for (const [name, m] of collector.commandMetrics) {
      if (m.count() === 0 && m.errors === 0) continue;
      this.log.info(
        `  ${name}: ${m.requests} req (${m.errors} err) | ` +
          `p50=${m.percentile(50)}us p95=${m.percentile(95)}us p99=${m.percentile(99)}us ` +
          `p99.9=${m.percentile(99.9)}us | min=${m.min()}us max=${m.max()}us`,
      );
    }
  }
}
