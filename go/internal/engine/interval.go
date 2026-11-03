package engine

import (
	"sync"
	"time"

	"github.com/resp-bench/go/internal/command"
	"github.com/resp-bench/go/internal/config"
	"github.com/resp-bench/go/internal/metrics"
)

// windowSlot is one worker's current-window collector. The worker records into
// it under mu; the reporter swaps it for a fresh collector under the same mu.
// Each worker owns its slot, so the lock is only ever contended at a swap.
type windowSlot struct {
	mu sync.Mutex
	c  *metrics.Collector
}

func (w *windowSlot) record(r command.Result) {
	w.mu.Lock()
	w.c.Record(r)
	w.mu.Unlock()
}

// swap returns the current window collector and installs a fresh one.
func (w *windowSlot) swap() *metrics.Collector {
	fresh := metrics.NewCollector()
	w.mu.Lock()
	old := w.c
	w.c = fresh
	w.mu.Unlock()
	return old
}

// intervalReporter emits one interval record per reporting_interval_seconds
// while a phase's workload runs, plus a final partial interval at phase end, so
// interval request totals sum to the phase total. The cumulative per-worker
// collectors are untouched.
type intervalReporter struct {
	b           *Benchmark
	phaseID     string
	connections int
	period      time.Duration
	slots       []*windowSlot

	workloadStart time.Time
	windowStart   time.Time
	index         int
	disabled      bool // sidecar could not be opened; keep recording cheap but write nothing

	stop chan struct{}
	done chan struct{}
}

func (b *Benchmark) newIntervalReporter(phase config.PhaseConfig, connections, workers int) *intervalReporter {
	r := &intervalReporter{
		b:           b,
		phaseID:     phase.ID,
		connections: connections,
		period:      time.Duration(phase.ReportingIntervalSeconds) * time.Second,
		slots:       make([]*windowSlot, workers),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	for i := range r.slots {
		r.slots[i] = &windowSlot{c: metrics.NewCollector()}
	}
	if err := b.intervalWriter.Open(); err != nil {
		b.logger.Printf("interval reporting disabled for phase %s: %v", phase.ID, err)
		r.disabled = true
	}
	// The interval clock starts with the workload loop (after connect/warmup).
	r.workloadStart = time.Now()
	r.windowStart = r.workloadStart
	return r
}

// start launches the periodic emitter.
func (r *intervalReporter) start() {
	go func() {
		defer close(r.done)
		t := time.NewTicker(r.period)
		defer t.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-t.C:
				r.emit(false)
			}
		}
	}()
}

// finish stops the emitter (after all workers have exited) and writes the final
// partial interval if it recorded any request.
func (r *intervalReporter) finish() {
	close(r.stop)
	<-r.done
	r.emit(true)
	if err := r.b.intervalWriter.Close(); err != nil {
		r.b.logger.Printf("close interval file: %v", err)
	}
}

// emit snapshots every worker's window and writes one record. Only one
// goroutine calls emit at a time (the ticker goroutine, then finish after it
// has exited), so index/windowStart need no extra locking.
func (r *intervalReporter) emit(final bool) {
	end := time.Now()
	window := metrics.NewCollector()
	for _, s := range r.slots {
		window.MergeFrom(s.swap())
	}
	if final && window.TotalRequests() == 0 {
		return
	}
	win := metrics.IntervalWindow{Index: r.index, Start: r.windowStart, End: end, WorkloadStart: r.workloadStart}
	r.index++
	r.windowStart = end
	if r.disabled {
		return
	}
	if err := r.b.intervalWriter.WriteInterval(r.phaseID, r.connections, win, window, metrics.ReadMemoryStats()); err != nil {
		r.b.logger.Printf("interval record: %v", err)
	}
}
