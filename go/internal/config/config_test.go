package config

import "testing"

// TestWarmupRequestsAbsentVsExplicitZero: an absent warmup_requests defaults to
// 1, while an explicit 0 disables warmup. Promoting it to *int is what lets the
// Go engine match Java/Node/PHP/Python, which apply the default only on absence.
func TestWarmupRequestsAbsentVsExplicitZero(t *testing.T) {
	absent, err := ParseWorkloadConfig([]byte(`{
	  "benchmark_profile": {"name": "W"},
	  "phases": [{"id":"P","connections":1,
	    "commands":[{"command":"get","weight":1.0}],
	    "keyspace":{"keys_count":10,"key_size_bytes":8,"key_prefix":"w:","generation_alg":"sequential_int"},
	    "completion":{"type":"requests","requests":1}}]
	}`))
	if err != nil {
		t.Fatalf("parse absent: %v", err)
	}
	if got := absent.Phases[0].EffectiveWarmupRequests(); got != 1 {
		t.Fatalf("absent warmup_requests: got %d, want default 1", got)
	}

	zero, err := ParseWorkloadConfig([]byte(`{
	  "benchmark_profile": {"name": "W"},
	  "phases": [{"id":"P","connections":1,"warmup_requests":0,
	    "commands":[{"command":"get","weight":1.0}],
	    "keyspace":{"keys_count":10,"key_size_bytes":8,"key_prefix":"w:","generation_alg":"sequential_int"},
	    "completion":{"type":"requests","requests":1}}]
	}`))
	if err != nil {
		t.Fatalf("parse zero: %v", err)
	}
	if got := zero.Phases[0].EffectiveWarmupRequests(); got != 0 {
		t.Fatalf("explicit warmup_requests=0: got %d, want 0 (disabled)", got)
	}

	explicit, err := ParseWorkloadConfig([]byte(`{
	  "benchmark_profile": {"name": "W"},
	  "phases": [{"id":"P","connections":1,"warmup_requests":5,
	    "commands":[{"command":"get","weight":1.0}],
	    "keyspace":{"keys_count":10,"key_size_bytes":8,"key_prefix":"w:","generation_alg":"sequential_int"},
	    "completion":{"type":"requests","requests":1}}]
	}`))
	if err != nil {
		t.Fatalf("parse explicit: %v", err)
	}
	if got := explicit.Phases[0].EffectiveWarmupRequests(); got != 5 {
		t.Fatalf("explicit warmup_requests=5: got %d, want 5", got)
	}
}

// TestReportingIntervalParsing: reporting_interval_seconds defaults to 0
// (disabled), parses positive integers, and rejects non-integers at parse time.
// Negative values parse but are rejected by engine validation (validateKnobs).
func TestReportingIntervalParsing(t *testing.T) {
	phase := func(extra string) string {
		return `{"benchmark_profile":{"name":"R"},"phases":[{"id":"P","connections":1` + extra + `,
		  "commands":[{"command":"get","weight":1.0}],
		  "keyspace":{"keys_count":10,"key_size_bytes":8},
		  "completion":{"type":"duration","seconds":1}}]}`
	}
	absent, err := ParseWorkloadConfig([]byte(phase("")))
	if err != nil {
		t.Fatalf("parse absent: %v", err)
	}
	if p := absent.Phases[0]; p.ReportingIntervalSeconds != 0 || p.IntervalReportingEnabled() {
		t.Fatalf("absent: got %d enabled=%v, want 0/disabled", p.ReportingIntervalSeconds, p.IntervalReportingEnabled())
	}
	zero, err := ParseWorkloadConfig([]byte(phase(`,"reporting_interval_seconds":0`)))
	if err != nil || zero.Phases[0].IntervalReportingEnabled() {
		t.Fatalf("explicit 0 must parse and be disabled (err=%v)", err)
	}
	set, err := ParseWorkloadConfig([]byte(phase(`,"reporting_interval_seconds":30`)))
	if err != nil {
		t.Fatalf("parse 30: %v", err)
	}
	if p := set.Phases[0]; p.ReportingIntervalSeconds != 30 || !p.IntervalReportingEnabled() {
		t.Fatalf("30: got %d enabled=%v", p.ReportingIntervalSeconds, p.IntervalReportingEnabled())
	}
	for _, bad := range []string{`1.5`, `"30"`} {
		if _, err := ParseWorkloadConfig([]byte(phase(`,"reporting_interval_seconds":` + bad))); err == nil {
			t.Fatalf("reporting_interval_seconds=%s: expected parse error", bad)
		}
	}
	neg, err := ParseWorkloadConfig([]byte(phase(`,"reporting_interval_seconds":-5`)))
	if err != nil {
		t.Fatalf("parse -5: %v", err)
	}
	if neg.Phases[0].IntervalReportingEnabled() {
		t.Fatal("negative must not enable interval reporting")
	}
}
