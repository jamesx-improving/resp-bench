/** Configuration for a single benchmark phase. */

import type { CommandConfig } from './commandConfig.js';
import type { CompletionConfig } from './completionConfig.js';
import type { KeyspaceConfig } from './keyspaceConfig.js';

export const DEFAULT_PIPELINE_DEPTH = 1;
export const DEFAULT_WARMUP_REQUESTS = 1;

export class PhaseConfig {
  readonly id: string;
  readonly description: string | null;
  readonly connections: number;
  readonly completion: CompletionConfig;
  readonly keyspace: KeyspaceConfig;
  readonly commands: CommandConfig[];
  readonly cpsLimit: number;
  readonly rpsLimit: number;
  readonly pipelineDepth: number;
  readonly warmupRequests: number;
  /** Interval-reporting period in seconds; 0 = disabled (docs/INTERVAL_REPORTING.md). */
  readonly reportingIntervalSeconds: number;

  constructor(init: {
    id: string;
    description?: string | null;
    connections: number;
    completion: CompletionConfig;
    keyspace: KeyspaceConfig;
    commands: CommandConfig[];
    cpsLimit?: number | null;
    rpsLimit?: number | null;
    pipelineDepth?: number | null;
    warmupRequests?: number | null;
    reportingIntervalSeconds?: number | null;
  }) {
    this.id = init.id;
    this.description = init.description ?? null;
    this.connections = init.connections;
    this.completion = init.completion;
    this.keyspace = init.keyspace;
    this.commands = init.commands;
    // -1 is the configs' "unlimited" sentinel; null/undefined means the same.
    this.cpsLimit = init.cpsLimit ?? -1;
    this.rpsLimit = init.rpsLimit ?? -1;
    this.pipelineDepth = init.pipelineDepth ?? DEFAULT_PIPELINE_DEPTH;
    this.warmupRequests = init.warmupRequests ?? DEFAULT_WARMUP_REQUESTS;
    this.reportingIntervalSeconds = init.reportingIntervalSeconds ?? 0;
  }

  hasCpsLimit(): boolean {
    return this.cpsLimit > 0;
  }

  hasRpsLimit(): boolean {
    return this.rpsLimit > 0;
  }

  hasIntervalReporting(): boolean {
    return this.reportingIntervalSeconds > 0;
  }

  effectivePipelineDepth(): number {
    return this.pipelineDepth > 0 ? this.pipelineDepth : DEFAULT_PIPELINE_DEPTH;
  }
}
