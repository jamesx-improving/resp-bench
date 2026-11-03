// Package config parses resp-bench driver and workload JSON configuration.
//
// The field names and defaults mirror the reference engines (Java/Ruby/Node/PHP)
// so the same config files drive every engine identically.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// DriverConfig describes which client library to use and how to connect.
type DriverConfig struct {
	SchemaVersion        string         `json:"schema_version"`
	Description          string         `json:"description"`
	DriverID             string         `json:"driver_id"`
	Mode                 string         `json:"mode"` // standalone | cluster | sentinel
	CommandTimeoutMs     *int           `json:"command_timeout_ms,omitempty"`
	TLS                  map[string]any `json:"tls,omitempty"`
	Auth                 map[string]any `json:"auth,omitempty"`
	SpecificDriverConfig map[string]any `json:"specific_driver_config,omitempty"`
}

// SecondaryDriverID returns specific_driver_config.secondary_driver_id if present.
func (d DriverConfig) SecondaryDriverID() string {
	if d.SpecificDriverConfig == nil {
		return ""
	}
	if v, ok := d.SpecificDriverConfig["secondary_driver_id"].(string); ok {
		return v
	}
	return ""
}

// BenchmarkProfile is the descriptive header of a workload.
type BenchmarkProfile struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Version     string `json:"version"`
}

// WorkloadConfig is the top-level workload definition.
type WorkloadConfig struct {
	SchemaVersion    string           `json:"schema_version"`
	BenchmarkProfile BenchmarkProfile `json:"benchmark_profile"`
	Phases           []PhaseConfig    `json:"phases"`
}

// Name is a convenience accessor for the profile name.
func (w WorkloadConfig) Name() string { return w.BenchmarkProfile.Name }

// PhaseConfig is a single phase of a workload.
type PhaseConfig struct {
	ID             string           `json:"id"`
	Description    string           `json:"description"`
	Connections    int              `json:"connections"`
	CpsLimit       int              `json:"cps_limit"`
	RpsLimit       int              `json:"rps_limit"`
	PipelineDepth  int              `json:"pipeline_depth"`
	WarmupRequests *int             `json:"warmup_requests"`
	Completion     CompletionConfig `json:"completion"`
	Keyspace       KeyspaceConfig   `json:"keyspace"`
	Commands       []CommandConfig  `json:"commands"`
	// ReportingIntervalSeconds enables interval (soak-test time series) records
	// every N seconds; 0/absent disables. Negative is a validation error and a
	// non-integer fails JSON parsing. See docs/INTERVAL_REPORTING.md.
	ReportingIntervalSeconds int `json:"reporting_interval_seconds"`
}

// IntervalReportingEnabled reports whether interval records should be emitted.
func (p PhaseConfig) IntervalReportingEnabled() bool { return p.ReportingIntervalSeconds > 0 }

// HasRpsLimit reports whether a positive rps limit is configured.
func (p PhaseConfig) HasRpsLimit() bool { return p.RpsLimit > 0 }

// HasCpsLimit reports whether a positive cps limit is configured.
func (p PhaseConfig) HasCpsLimit() bool { return p.CpsLimit > 0 }

// EffectiveWarmupRequests returns the configured warmup request count, applying
// the default of 1 only when the key is absent. An explicit 0 disables warmup,
// matching the schema ("minimum: 0, default: 1") and the other engines, which
// apply the default on absence only.
func (p PhaseConfig) EffectiveWarmupRequests() int {
	if p.WarmupRequests == nil {
		return 1
	}
	return *p.WarmupRequests
}

// EffectivePipelineDepth defaults to 1 when unset/invalid.
func (p PhaseConfig) EffectivePipelineDepth() int {
	if p.PipelineDepth < 1 {
		return 1
	}
	return p.PipelineDepth
}

// CompletionConfig defines when a phase stops.
type CompletionConfig struct {
	Type     string `json:"type"` // "requests" | "duration"
	Seconds  int    `json:"seconds"`
	Requests int    `json:"requests"`
}

// IsDurationBased reports whether the phase runs for a wall-clock duration.
func (c CompletionConfig) IsDurationBased() bool {
	return strings.EqualFold(c.Type, "duration")
}

// IsRequestBased reports whether the phase runs for a fixed request count.
func (c CompletionConfig) IsRequestBased() bool {
	return strings.EqualFold(c.Type, "requests")
}

// KeyspaceConfig controls key generation.
type KeyspaceConfig struct {
	KeysCount     int    `json:"keys_count"`
	KeySizeBytes  int    `json:"key_size_bytes"`
	KeyPrefix     string `json:"key_prefix"`
	GenerationAlg string `json:"generation_alg"` // "sequential_int" | "uniform_rand"
	Seed          *int64 `json:"seed,omitempty"`
}

// EffectiveKeyPrefix returns the prefix or a default matching the reference engines.
func (k KeyspaceConfig) EffectiveKeyPrefix() string {
	if k.KeyPrefix == "" {
		return "key:"
	}
	return k.KeyPrefix
}

// SeedValue returns the configured seed or the shared default (12345).
func (k KeyspaceConfig) SeedValue() int64 {
	if k.Seed == nil {
		return 12345
	}
	return *k.Seed
}

// IsSequentialInt reports the sequential_int algorithm (the default).
func (k KeyspaceConfig) IsSequentialInt() bool {
	return k.GenerationAlg == "" || k.GenerationAlg == "sequential_int"
}

// CommandConfig is a single weighted command in a phase.
type CommandConfig struct {
	Command       string  `json:"command"`
	Weight        float64 `json:"weight"`
	DataSizeBytes int     `json:"data_size_bytes"`
}

// LoadDriverConfig reads and parses a driver config file.
func LoadDriverConfig(path string) (DriverConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return DriverConfig{}, fmt.Errorf("read driver config: %w", err)
	}
	return ParseDriverConfig(data)
}

// ParseDriverConfig parses driver config from raw JSON, applying defaults.
func ParseDriverConfig(data []byte) (DriverConfig, error) {
	var d DriverConfig
	if err := json.Unmarshal(data, &d); err != nil {
		return DriverConfig{}, fmt.Errorf("parse driver config: %w", err)
	}
	if d.SchemaVersion == "" {
		d.SchemaVersion = "1.0"
	}
	if d.Mode == "" {
		d.Mode = "standalone"
	}
	if d.SpecificDriverConfig == nil {
		d.SpecificDriverConfig = map[string]any{}
	}
	return d, nil
}

// LoadWorkloadConfig reads and parses a workload config file.
func LoadWorkloadConfig(path string) (WorkloadConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return WorkloadConfig{}, fmt.Errorf("read workload config: %w", err)
	}
	return ParseWorkloadConfig(data)
}

// ParseWorkloadConfig parses workload config from raw JSON, applying per-phase defaults.
func ParseWorkloadConfig(data []byte) (WorkloadConfig, error) {
	var w WorkloadConfig
	if err := json.Unmarshal(data, &w); err != nil {
		return WorkloadConfig{}, fmt.Errorf("parse workload config: %w", err)
	}
	if w.SchemaVersion == "" {
		w.SchemaVersion = "1.0"
	}
	for i := range w.Phases {
		p := &w.Phases[i]
		// Defaults matching the reference engines: rps/cps default to -1 (unlimited),
		// pipeline_depth to 1. warmup_requests defaults to 1 only when absent (an
		// explicit 0 disables warmup), so it is handled via EffectiveWarmupRequests.
		if p.CpsLimit == 0 {
			p.CpsLimit = -1
		}
		if p.RpsLimit == 0 {
			p.RpsLimit = -1
		}
		if p.PipelineDepth == 0 {
			p.PipelineDepth = 1
		}
	}
	return w, nil
}
