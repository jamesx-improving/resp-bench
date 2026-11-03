/**
 * Loads driver and workload configuration from the shared configs/ JSON files.
 *
 * Field names and defaults mirror the Java/Ruby/Python engines exactly, so the
 * same JSON is consumed identically by every engine. Unlike those engines this
 * one also validates the required fields up front: a typo'd config should fail
 * with a clear message rather than surface later as a null-shaped error deep in
 * a worker loop.
 */

import { readFileSync } from 'node:fs';

import { CommandConfig } from './commandConfig.js';
import { CompletionConfig } from './completionConfig.js';
import { DriverConfig } from './driverConfig.js';
import { KeyspaceConfig } from './keyspaceConfig.js';
import { PhaseConfig } from './phaseConfig.js';
import { WorkloadConfig } from './workloadConfig.js';

type Json = Record<string, unknown>;

export class ConfigError extends Error {}

function asRecord(value: unknown, what: string): Json {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new ConfigError(`${what} must be a JSON object`);
  }
  return value as Json;
}

function optString(data: Json, key: string): string | null {
  const value = data[key];
  if (value === undefined || value === null) return null;
  if (typeof value !== 'string') throw new ConfigError(`"${key}" must be a string`);
  return value;
}

function reqString(data: Json, key: string, what: string): string {
  const value = optString(data, key);
  if (value === null || value === '') throw new ConfigError(`${what} is missing required "${key}"`);
  return value;
}

function optNumber(data: Json, key: string): number | null {
  const value = data[key];
  if (value === undefined || value === null) return null;
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new ConfigError(`"${key}" must be a finite number`);
  }
  return value;
}

function reqInt(data: Json, key: string, what: string): number {
  const value = optNumber(data, key);
  if (value === null) throw new ConfigError(`${what} is missing required "${key}"`);
  if (!Number.isInteger(value)) throw new ConfigError(`"${key}" must be an integer`);
  return value;
}

function readJson(path: string, what: string): Json {
  let text: string;
  try {
    text = readFileSync(path, 'utf8');
  } catch (error) {
    throw new ConfigError(`cannot read ${what} "${path}": ${(error as Error).message}`);
  }
  try {
    return asRecord(JSON.parse(text), what);
  } catch (error) {
    if (error instanceof ConfigError) throw error;
    throw new ConfigError(`${what} "${path}" is not valid JSON: ${(error as Error).message}`);
  }
}

export class ConfigLoader {
  static loadDriverConfig(path: string): DriverConfig {
    return ConfigLoader.parseDriverConfig(readJson(path, 'driver config'));
  }

  static loadWorkloadConfig(path: string): WorkloadConfig {
    return ConfigLoader.parseWorkloadConfig(readJson(path, 'workload config'));
  }

  static parseDriverConfig(data: Json): DriverConfig {
    const mode = optString(data, 'mode') ?? 'standalone';
    if (!['standalone', 'cluster', 'sentinel'].includes(mode)) {
      throw new ConfigError(
        `driver config "mode" must be standalone, cluster or sentinel (got "${mode}")`,
      );
    }
    return new DriverConfig({
      schemaVersion: optString(data, 'schema_version') ?? '1.0',
      description: optString(data, 'description'),
      driverId: reqString(data, 'driver_id', 'driver config'),
      mode,
      commandTimeoutMs: optNumber(data, 'command_timeout_ms'),
      tls: (data['tls'] ?? null) as DriverConfig['tls'],
      auth: (data['auth'] ?? null) as DriverConfig['auth'],
      specificDriverConfig: (data['specific_driver_config'] ?? {}) as Record<string, unknown>,
    });
  }

  static parseWorkloadConfig(data: Json): WorkloadConfig {
    const rawPhases = data['phases'];
    if (!Array.isArray(rawPhases) || rawPhases.length === 0) {
      throw new ConfigError('workload config must have a non-empty "phases" array');
    }
    return new WorkloadConfig({
      schemaVersion: optString(data, 'schema_version') ?? '1.0',
      benchmarkProfile: (data['benchmark_profile'] ?? {}) as WorkloadConfig['benchmarkProfile'],
      phases: rawPhases.map((phase, index) =>
        ConfigLoader.parsePhase(asRecord(phase, `phases[${index}]`), index),
      ),
    });
  }

  static parsePhase(data: Json, index = 0): PhaseConfig {
    const what = `phases[${index}]`;
    const rawCommands = data['commands'];
    if (!Array.isArray(rawCommands) || rawCommands.length === 0) {
      throw new ConfigError(`${what} must have a non-empty "commands" array`);
    }
    const connections = reqInt(data, 'connections', what);
    if (connections <= 0) throw new ConfigError(`${what} "connections" must be positive`);
    const reportingIntervalSeconds = optNumber(data, 'reporting_interval_seconds');
    if (
      reportingIntervalSeconds !== null &&
      (!Number.isInteger(reportingIntervalSeconds) || reportingIntervalSeconds < 0)
    ) {
      throw new ConfigError(
        `${what} "reporting_interval_seconds" must be a non-negative integer ` +
          `(got ${reportingIntervalSeconds})`,
      );
    }

    return new PhaseConfig({
      id: reqString(data, 'id', what),
      description: optString(data, 'description'),
      connections,
      cpsLimit: optNumber(data, 'cps_limit'),
      rpsLimit: optNumber(data, 'rps_limit'),
      pipelineDepth: optNumber(data, 'pipeline_depth'),
      warmupRequests: optNumber(data, 'warmup_requests'),
      reportingIntervalSeconds,
      completion: ConfigLoader.parseCompletion(
        asRecord(data['completion'] ?? {}, `${what}.completion`),
        what,
      ),
      keyspace: ConfigLoader.parseKeyspace(
        asRecord(data['keyspace'] ?? {}, `${what}.keyspace`),
        what,
      ),
      commands: rawCommands.map((command, i) =>
        ConfigLoader.parseCommand(asRecord(command, `${what}.commands[${i}]`), `${what}.commands[${i}]`),
      ),
    });
  }

  static parseCompletion(data: Json, what = 'completion'): CompletionConfig {
    const type = reqString(data, 'type', `${what}.completion`);
    if (!['duration', 'requests'].includes(type)) {
      throw new ConfigError(
        `${what}.completion "type" must be duration or requests (got "${type}")`,
      );
    }
    const seconds = optNumber(data, 'seconds');
    const requests = optNumber(data, 'requests');
    if (type === 'duration' && (seconds === null || seconds <= 0)) {
      throw new ConfigError(`${what}.completion type=duration requires a positive "seconds"`);
    }
    if (type === 'requests' && (requests === null || requests <= 0)) {
      throw new ConfigError(`${what}.completion type=requests requires a positive "requests"`);
    }
    return new CompletionConfig({ type, seconds, requests });
  }

  static parseKeyspace(data: Json, what = 'keyspace'): KeyspaceConfig {
    const keysCount = reqInt(data, 'keys_count', `${what}.keyspace`);
    if (keysCount <= 0) throw new ConfigError(`${what}.keyspace "keys_count" must be positive`);
    const generationAlg = optString(data, 'generation_alg') ?? 'sequential_int';
    if (!['sequential_int', 'uniform_rand'].includes(generationAlg)) {
      throw new ConfigError(
        `${what}.keyspace "generation_alg" must be sequential_int or uniform_rand ` +
          `(got "${generationAlg}")`,
      );
    }
    return new KeyspaceConfig({
      keysCount,
      keySizeBytes: optNumber(data, 'key_size_bytes'),
      keyPrefix: optString(data, 'key_prefix'),
      generationAlg,
      seed: optNumber(data, 'seed'),
    });
  }

  static parseCommand(data: Json, what = 'command'): CommandConfig {
    const command = reqString(data, 'command', what);
    const weight = optNumber(data, 'weight');
    if (weight !== null && (weight < 0 || weight > 1)) {
      // Matches Java's CommandConfig.validate(): weights are fractions of 1.
      throw new ConfigError(`${what} "weight" must be between 0 and 1 (got ${weight})`);
    }
    return new CommandConfig({
      command,
      weight,
      dataSizeBytes: optNumber(data, 'data_size_bytes'),
    });
  }
}
