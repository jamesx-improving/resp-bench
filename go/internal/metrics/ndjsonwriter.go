package metrics

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// NdjsonWriter appends one JSON line per phase, matching the schema used by the
// Java/Ruby/Python/PHP/Node engines exactly.
type NdjsonWriter struct {
	path string
	mu   sync.Mutex

	commitID               string
	driverID               string
	primaryDriverVersion   string
	secondaryDriverID      string
	secondaryDriverVersion string
	driverDetails          map[string]any
	hasMetadata            bool
}

// NewNdjsonWriter creates a writer for the given output path.
func NewNdjsonWriter(path string) *NdjsonWriter {
	return &NdjsonWriter{path: path}
}

// SetMetadata records the run metadata written into every phase line. Extra
// driverDetails keys (negotiated protocol, parser, pipelining mode) are merged
// into the metadata block; downstream tooling reads metadata by key, so extra
// keys are harmless to engines that omit them.
func (w *NdjsonWriter) SetMetadata(commitID, driverID, primaryVersion, secondaryID, secondaryVersion string, driverDetails map[string]any) {
	w.commitID = commitID
	w.driverID = driverID
	w.primaryDriverVersion = primaryVersion
	w.secondaryDriverID = secondaryID
	w.secondaryDriverVersion = secondaryVersion
	w.driverDetails = driverDetails
	w.hasMetadata = true
}

// summary is the latency percentile summary block.
type summary struct {
	Min  int64 `json:"min"`
	P50  int64 `json:"p50"`
	P95  int64 `json:"p95"`
	P99  int64 `json:"p99"`
	P999 int64 `json:"p999"`
	Max  int64 `json:"max"`
}

type hdrBlock struct {
	Format     string `json:"format"`
	Sigfig     int    `json:"sigfig"`
	PayloadB64 string `json:"payload_b64"`
}

type latencyBlock struct {
	Unit    string    `json:"unit"`
	Count   int64     `json:"count"`
	Summary summary   `json:"summary"`
	HDR     *hdrBlock `json:"hdr,omitempty"`
}

type commandBlock struct {
	Requests int64        `json:"requests"`
	Errors   int64        `json:"errors"`
	Latency  latencyBlock `json:"latency"`
}

type phaseBlock struct {
	ID              string  `json:"id"`
	Status          string  `json:"status"`
	StartTimestamp  *string `json:"start_timestamp"`
	FinishTimestamp *string `json:"finish_timestamp"`
	DurationMs      int64   `json:"duration_ms"`
	Connections     int     `json:"connections"`
	// Additive, optional fields. `connections` alone cannot distinguish a
	// pipelined run from a serial one, and the concurrency actually applied is
	// connections x pipeline_depth. sockets_per_client is 1 for a multiplexing
	// driver at any depth, pipeline_depth for a pooling one. Other engines omit
	// these and downstream tooling reads by key, so they are ignored there.
	PipelineDepth    int `json:"pipeline_depth"`
	SocketsPerClient int `json:"sockets_per_client"`
	TotalSockets     int `json:"total_sockets"`
}

// phaseRecord is the full per-phase JSON object. Metadata is a free-form map so
// driver-detail keys can be merged alongside the fixed keys. Metrics is always a
// JSON object ({} when empty), never an array, because consumers index into it.
type phaseRecord struct {
	Metadata map[string]any          `json:"metadata,omitempty"`
	Phase    phaseBlock              `json:"phase"`
	Totals   map[string]int64        `json:"totals"`
	Metrics  map[string]commandBlock `json:"metrics"`
}

// WritePhaseResults appends a phase record built from the collector.
func (w *NdjsonWriter) WritePhaseResults(phaseID, status string, connections, pipelineDepth, socketsPerClient int, c *Collector) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(w.path), 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	rec := w.buildRecord(phaseID, status, connections, pipelineDepth, socketsPerClient, c)
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal phase record: %w", err)
	}

	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open metrics file: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write metrics line: %w", err)
	}
	return nil
}

func (w *NdjsonWriter) buildRecord(phaseID, status string, connections, pipelineDepth, socketsPerClient int, c *Collector) phaseRecord {
	if socketsPerClient < 1 {
		socketsPerClient = 1
	}
	rec := phaseRecord{
		Phase: phaseBlock{
			ID:               phaseID,
			Status:           status,
			StartTimestamp:   iso8601(c.StartTime()),
			FinishTimestamp:  iso8601(c.StopTime()),
			DurationMs:       c.DurationMillis(),
			Connections:      connections,
			PipelineDepth:    pipelineDepth,
			SocketsPerClient: socketsPerClient,
			TotalSockets:     connections * socketsPerClient,
		},
		Totals: map[string]int64{
			"requests": c.TotalRequests(),
			"errors":   c.TotalErrors(),
		},
	}

	if w.hasMetadata {
		md := map[string]any{
			"timestamp": time.Now().UTC().Format("2006-01-02T15:04:05Z"),
		}
		if w.commitID != "" {
			md["commit_id"] = w.commitID
		}
		if w.driverID != "" {
			md["driver_id"] = w.driverID
		}
		if w.primaryDriverVersion != "" {
			md["primary_driver_version"] = w.primaryDriverVersion
		}
		if w.secondaryDriverID != "" {
			md["secondary_driver_id"] = w.secondaryDriverID
		}
		if w.secondaryDriverVersion != "" {
			md["secondary_driver_version"] = w.secondaryDriverVersion
		}
		// Merge driver-detail keys without overwriting the fixed keys.
		for k, v := range w.driverDetails {
			if _, exists := md[k]; !exists {
				md[k] = v
			}
		}
		rec.Metadata = md
	}

	rec.Metrics = commandBlocks(c)
	return rec
}

// commandBlocks renders a collector's per-command metrics in the phase-record
// shape (same keys, naming, units and HDR encoding). Shared by the phase record
// and the interval record so the two can never drift apart. Always non-nil so it
// marshals as {} when empty.
func commandBlocks(c *Collector) map[string]commandBlock {
	out := map[string]commandBlock{}
	for name, cm := range c.Commands() {
		block := commandBlock{
			Requests: cm.Requests,
			Errors:   cm.Errors,
			Latency: latencyBlock{
				Unit:  "us",
				Count: cm.Histogram.TotalCount(),
				Summary: summary{
					Min:  cm.Histogram.Min(),
					P50:  cm.Histogram.ValueAtPercentile(50),
					P95:  cm.Histogram.ValueAtPercentile(95),
					P99:  cm.Histogram.ValueAtPercentile(99),
					P999: cm.Histogram.ValueAtPercentile(99.9),
					Max:  cm.Histogram.Max(),
				},
			},
		}
		if b64, err := EncodeCompressedBase64(cm.Histogram); err == nil {
			block.Latency.HDR = &hdrBlock{Format: "hdr", Sigfig: 3, PayloadB64: b64}
		}
		out[name] = block
	}
	return out
}

// iso8601 formats a UTC timestamp, or nil if the time is zero.
func iso8601(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := t.UTC().Format("2006-01-02T15:04:05Z")
	return &s
}
