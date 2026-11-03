package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/resp-bench/go/internal/config"
)

func readNdjson(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var recs []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("parse line %q: %v", line, err)
		}
		recs = append(recs, m)
	}
	return recs
}

// TestIntervalReportingEndToEnd: a 3s duration phase with a 1s interval writes
// sidecar records with increasing indexes whose request totals sum exactly to
// the phase total; the metrics file still holds only the phase record.
func TestIntervalReportingEndToEnd(t *testing.T) {
	wl, err := config.ParseWorkloadConfig([]byte(`{
	  "benchmark_profile": {"name": "SOAK"},
	  "phases": [{"id":"STEADY","connections":2,"pipeline_depth":2,"reporting_interval_seconds":1,
	    "rps_limit":2000,
	    "commands":[{"command":"get","weight":0.7},{"command":"set","weight":0.3,"data_size_bytes":16}],
	    "keyspace":{"keys_count":100,"key_size_bytes":16,"key_prefix":"s:","generation_alg":"sequential_int"},
	    "completion":{"type":"duration","seconds":3}}]
	}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	dir := t.TempDir()
	out := filepath.Join(dir, "glide@connections=2.ndjson")
	b := New("localhost", 6379, recordingDriver(), wl, out, "t", quietLogger())
	code, err := b.Run()
	if err != nil || code != 0 {
		t.Fatalf("run: code=%d err=%v", code, err)
	}

	phaseRecs := readNdjson(t, out)
	if len(phaseRecs) != 1 {
		t.Fatalf("metrics file: want 1 phase record, got %d", len(phaseRecs))
	}
	phaseTotal := int64(phaseRecs[0]["totals"].(map[string]any)["requests"].(float64))
	phaseCmd := phaseRecs[0]["metrics"].(map[string]any)

	ivs := readNdjson(t, filepath.Join(dir, "glide@connections=2.interval.ndjson"))
	if len(ivs) < 3 || len(ivs) > 4 {
		t.Fatalf("want 3-4 interval records for 3s/1s, got %d", len(ivs))
	}
	var sum int64
	perCmd := map[string]int64{}
	for i, r := range ivs {
		if r["type"] != "interval" || r["phase_id"] != "STEADY" {
			t.Fatalf("record %d: bad type/phase_id: %v", i, r)
		}
		iv := r["interval"].(map[string]any)
		if got := int(iv["index"].(float64)); got != i {
			t.Fatalf("record %d: index=%d", i, got)
		}
		if i > 0 && iv["start_timestamp"] != ivs[i-1]["interval"].(map[string]any)["end_timestamp"] {
			t.Fatalf("record %d: windows not contiguous", i)
		}
		for _, k := range []string{"elapsed_s", "duration_s", "start_timestamp", "end_timestamp"} {
			if _, ok := iv[k]; !ok {
				t.Fatalf("record %d: interval missing %s", i, k)
			}
		}
		if int(r["connections"].(float64)) != 2 {
			t.Fatalf("record %d: connections=%v", i, r["connections"])
		}
		if _, ok := r["rps"]; !ok {
			t.Fatalf("record %d: missing rps", i)
		}
		mem := r["memory"].(map[string]any)
		for _, k := range []string{"heap_alloc_bytes", "heap_inuse_bytes", "heap_sys_bytes", "sys_bytes", "num_gc", "gc_pause_total_ns", "goroutines"} {
			if _, ok := mem[k]; !ok {
				t.Fatalf("record %d: memory missing %s", i, k)
			}
		}
		sum += int64(r["totals"].(map[string]any)["requests"].(float64))
		for name, v := range r["metrics"].(map[string]any) {
			if _, ok := phaseCmd[name]; !ok {
				t.Fatalf("record %d: command %q not in phase record", i, name)
			}
			cb := v.(map[string]any)
			lat := cb["latency"].(map[string]any)
			if lat["unit"] != "us" || lat["hdr"].(map[string]any)["payload_b64"] == "" {
				t.Fatalf("record %d: %s latency block malformed: %v", i, name, lat)
			}
			perCmd[name] += int64(cb["requests"].(float64))
		}
	}
	if sum != phaseTotal || phaseTotal == 0 {
		t.Fatalf("interval requests sum=%d, phase total=%d", sum, phaseTotal)
	}
	for name, v := range phaseCmd {
		if want := int64(v.(map[string]any)["requests"].(float64)); perCmd[name] != want {
			t.Fatalf("%s: interval sum=%d, phase=%d", name, perCmd[name], want)
		}
	}
}

// TestIntervalReportingDisabledNoSidecar: without reporting_interval_seconds no
// sidecar is created.
func TestIntervalReportingDisabledNoSidecar(t *testing.T) {
	dir := t.TempDir()
	wl, _ := config.ParseWorkloadConfig([]byte(strings.Replace(requestWorkload, "%d", "2", 1)))
	out := filepath.Join(dir, "m.ndjson")
	b := New("localhost", 6379, recordingDriver(), wl, out, "t", quietLogger())
	if code, err := b.Run(); err != nil || code != 0 {
		t.Fatalf("run: code=%d err=%v", code, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "m.interval.ndjson")); !os.IsNotExist(err) {
		t.Fatalf("sidecar must not exist when disabled (stat err=%v)", err)
	}
}

// TestNegativeReportingIntervalRejected: a negative value is a validation error.
func TestNegativeReportingIntervalRejected(t *testing.T) {
	wl, err := config.ParseWorkloadConfig([]byte(`{
	  "benchmark_profile": {"name": "NEG"},
	  "phases": [{"id":"P","connections":1,"reporting_interval_seconds":-1,
	    "commands":[{"command":"get","weight":1.0}],
	    "keyspace":{"keys_count":10,"key_size_bytes":8},
	    "completion":{"type":"requests","requests":10}}]
	}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	b := New("localhost", 6379, recordingDriver(), wl, filepath.Join(t.TempDir(), "n.ndjson"), "t", quietLogger())
	code, err := b.Run()
	if err == nil || code == 0 || !strings.Contains(err.Error(), "reporting_interval_seconds") {
		t.Fatalf("expected validation error, got code=%d err=%v", code, err)
	}
}
