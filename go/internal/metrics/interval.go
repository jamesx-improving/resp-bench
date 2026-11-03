package metrics

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// IntervalPath derives the interval sidecar path from the --metrics path: a
// trailing ".ndjson" becomes ".interval.ndjson"; any other path gets
// ".interval.ndjson" appended. See docs/INTERVAL_REPORTING.md.
func IntervalPath(metricsPath string) string {
	if strings.HasSuffix(metricsPath, ".ndjson") {
		return strings.TrimSuffix(metricsPath, ".ndjson") + ".interval.ndjson"
	}
	return metricsPath + ".interval.ndjson"
}

// IntervalWindow is the timing of one interval record.
type IntervalWindow struct {
	Index         int
	Start         time.Time
	End           time.Time
	WorkloadStart time.Time
}

type intervalBlock struct {
	Index          int     `json:"index"`
	StartTimestamp string  `json:"start_timestamp"`
	EndTimestamp   string  `json:"end_timestamp"`
	ElapsedS       float64 `json:"elapsed_s"`
	DurationS      float64 `json:"duration_s"`
}

// MemoryStats is the Go `memory` block. RSSBytes is nil (omitted) when
// /proc/self/statm is not readable, e.g. on macOS.
type MemoryStats struct {
	RSSBytes       *int64 `json:"rss_bytes,omitempty"`
	HeapAllocBytes int64  `json:"heap_alloc_bytes"`
	HeapInuseBytes int64  `json:"heap_inuse_bytes"`
	HeapSysBytes   int64  `json:"heap_sys_bytes"`
	SysBytes       int64  `json:"sys_bytes"`
	NumGC          int64  `json:"num_gc"`
	GCPauseTotalNs int64  `json:"gc_pause_total_ns"`
	Goroutines     int64  `json:"goroutines"`
}

// ReadMemoryStats snapshots runtime memory. runtime.ReadMemStats briefly stops
// the world, so call it once per interval, not on the hot path.
func ReadMemoryStats() MemoryStats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m := MemoryStats{
		HeapAllocBytes: int64(ms.HeapAlloc),
		HeapInuseBytes: int64(ms.HeapInuse),
		HeapSysBytes:   int64(ms.HeapSys),
		SysBytes:       int64(ms.Sys),
		NumGC:          int64(ms.NumGC),
		GCPauseTotalNs: int64(ms.PauseTotalNs),
		Goroutines:     int64(runtime.NumGoroutine()),
	}
	if rss, ok := readRSS(); ok {
		m.RSSBytes = &rss
	}
	return m
}

// readRSS reads the resident set from /proc/self/statm (second field, in pages).
func readRSS() (int64, bool) {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return pages * int64(os.Getpagesize()), true
}

type intervalRecord struct {
	Type        string                  `json:"type"`
	PhaseID     string                  `json:"phase_id"`
	Interval    intervalBlock           `json:"interval"`
	Connections int                     `json:"connections"`
	Totals      map[string]int64        `json:"totals"`
	RPS         int64                   `json:"rps"`
	Metrics     map[string]commandBlock `json:"metrics"`
	Memory      MemoryStats             `json:"memory"`
}

// IntervalWriter appends interval records to the sidecar file. The file is
// opened (append, create) by Open and each record is a single unbuffered
// write, so every completed record reaches the OS before the next window.
type IntervalWriter struct {
	path string
	mu   sync.Mutex
	f    *os.File
}

// NewIntervalWriter creates a writer for the given sidecar path. Nothing is
// created on disk until Open.
func NewIntervalWriter(path string) *IntervalWriter { return &IntervalWriter{path: path} }

// Path returns the sidecar path.
func (w *IntervalWriter) Path() string { return w.path }

// Open opens (creating if needed) the sidecar in append mode. Idempotent.
func (w *IntervalWriter) Open() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(w.path), 0o755); err != nil {
		return fmt.Errorf("create interval output dir: %w", err)
	}
	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open interval file: %w", err)
	}
	w.f = f
	return nil
}

// Close closes the sidecar if open.
func (w *IntervalWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// WriteInterval appends one interval record built from a window-scoped collector.
func (w *IntervalWriter) WriteInterval(phaseID string, connections int, win IntervalWindow, c *Collector, mem MemoryStats) error {
	rec := buildIntervalRecord(phaseID, connections, win, c, mem)
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal interval record: %w", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return fmt.Errorf("interval writer not open")
	}
	if _, err := w.f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write interval line: %w", err)
	}
	return nil
}

func buildIntervalRecord(phaseID string, connections int, win IntervalWindow, c *Collector, mem MemoryStats) intervalRecord {
	dur := win.End.Sub(win.Start).Seconds()
	var rps int64
	if dur > 0 {
		rps = int64(math.Round(float64(c.TotalRequests()) / dur))
	}
	return intervalRecord{
		Type:    "interval",
		PhaseID: phaseID,
		Interval: intervalBlock{
			Index:          win.Index,
			StartTimestamp: isoMillis(win.Start),
			EndTimestamp:   isoMillis(win.End),
			ElapsedS:       round1(win.End.Sub(win.WorkloadStart).Seconds()),
			DurationS:      round1(dur),
		},
		Connections: connections,
		Totals: map[string]int64{
			"requests": c.TotalRequests(),
			"errors":   c.TotalErrors(),
		},
		RPS:     rps,
		Metrics: commandBlocks(c),
		Memory:  mem,
	}
}

func isoMillis(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

func round1(v float64) float64 { return math.Round(v*10) / 10 }
