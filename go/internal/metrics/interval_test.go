package metrics

import "testing"

func TestIntervalPath(t *testing.T) {
	cases := map[string]string{
		"results/x/valkey-glide@connections=64.ndjson": "results/x/valkey-glide@connections=64.interval.ndjson",
		"out.ndjson":     "out.interval.ndjson",
		"out.json":       "out.json.interval.ndjson",
		"out":            "out.interval.ndjson",
		"dir.ndjson/out": "dir.ndjson/out.interval.ndjson",
	}
	for in, want := range cases {
		if got := IntervalPath(in); got != want {
			t.Errorf("IntervalPath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestReadMemoryStats(t *testing.T) {
	m := ReadMemoryStats()
	if m.Goroutines < 1 || m.SysBytes <= 0 || m.HeapSysBytes <= 0 {
		t.Fatalf("implausible memory stats: %+v", m)
	}
	if _, ok := readRSS(); ok != (m.RSSBytes != nil) {
		t.Fatalf("rss_bytes presence must match /proc readability")
	}
}
