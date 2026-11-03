package engine

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/resp-bench/go/internal/client"
	"github.com/resp-bench/go/internal/command"
	"github.com/resp-bench/go/internal/config"
	"github.com/resp-bench/go/internal/metrics"
)

// Phase status strings written to the NDJSON output.
const (
	StatusCompleted   = "COMPLETED"
	StatusError       = "ERROR"
	StatusInterrupted = "INTERRUPTED"
)

// Benchmark orchestrates a workload against a driver, writing per-phase NDJSON.
//
// Concurrency model: one client per connection; each connection is driven by
// pipeline_depth worker goroutines that share that connection (so raising the
// depth adds in-flight requests without changing which keys a connection
// touches). For a request-based phase the budget is a single shared atomic
// counter claimed one request at a time (not pre-divided per worker), so a slow
// worker never leaves the target unmet. Each worker records into its own
// lock-free collector; collectors are merged after the phase, widening the
// window to min(start)/max(stop).
type Benchmark struct {
	host     string
	port     int
	driver   config.DriverConfig
	workload config.WorkloadConfig
	writer   *metrics.NdjsonWriter
	// intervalWriter targets the interval sidecar; the file is only opened when
	// a phase with reporting_interval_seconds > 0 starts.
	intervalWriter *metrics.IntervalWriter
	commitID       string
	logger         *log.Logger

	// interrupted is set (via SetInterrupted) when a signal handler wants the
	// current phase to stop early and be reported as INTERRUPTED.
	interrupted atomic.Bool
}

// New builds a Benchmark engine.
func New(host string, port int, driver config.DriverConfig, workload config.WorkloadConfig,
	metricsPath, commitID string, logger *log.Logger) *Benchmark {
	return &Benchmark{
		host:           host,
		port:           port,
		driver:         driver,
		workload:       workload,
		writer:         metrics.NewNdjsonWriter(metricsPath),
		intervalWriter: metrics.NewIntervalWriter(metrics.IntervalPath(metricsPath)),
		commitID:       commitID,
		logger:         logger,
	}
}

// SetInterrupted signals that running/remaining phases should stop early and be
// reported as INTERRUPTED. Safe to call from a signal handler goroutine.
func (b *Benchmark) SetInterrupted() { b.interrupted.Store(true) }

// Run executes all phases. It returns a non-zero exit code if any phase failed
// or was interrupted, so callers can propagate it as the process exit status.
func (b *Benchmark) Run() (int, error) {
	if err := b.validateKnobs(); err != nil {
		return 1, err
	}

	b.logger.Printf("Starting benchmark: %s", b.workload.Name())
	b.logger.Printf("Driver: %s, mode: %s, server: %s:%d", b.driver.DriverID, b.driver.Mode, b.host, b.port)

	b.setupMetadata()

	exitCode := 0
	for _, phase := range b.workload.Phases {
		status, err := b.executePhase(phase)
		if err != nil {
			b.logger.Printf("phase %s error: %v", phase.ID, err)
		}
		if status != StatusCompleted {
			exitCode = 1
		}
		if b.interrupted.Load() {
			break
		}
	}
	b.logger.Printf("Benchmark completed")
	return exitCode, nil
}

// validateKnobs rejects config values that are structurally invalid. Unlike the
// earlier version, pipeline_depth, cps_limit, and command_timeout_ms are now
// honored, so they are no longer rejected — only nonsensical values are.
func (b *Benchmark) validateKnobs() error {
	if b.driver.CommandTimeoutMs != nil && *b.driver.CommandTimeoutMs < 0 {
		return fmt.Errorf("command_timeout_ms must be >= 0")
	}
	for _, p := range b.workload.Phases {
		if p.PipelineDepth < 0 {
			return fmt.Errorf("phase %s: pipeline_depth must be >= 1", p.ID)
		}
		if p.ReportingIntervalSeconds < 0 {
			return fmt.Errorf("phase %s: reporting_interval_seconds must be >= 0", p.ID)
		}
		if len(p.Commands) == 0 {
			return fmt.Errorf("phase %s: commands must not be empty", p.ID)
		}
	}
	return nil
}

// setupMetadata connects a sample client to read the driver version and details.
// Failure is non-fatal (metadata falls back to "unknown").
func (b *Benchmark) setupMetadata() {
	sample, err := client.CreateAndConnect(b.host, b.port, b.driver, 1)
	if err != nil {
		b.logger.Printf("metadata: could not connect sample client: %v", err)
		b.writer.SetMetadata(b.commitID, b.driver.DriverID, "unknown", b.driver.SecondaryDriverID(), "", nil)
		return
	}
	defer sample.Close()
	b.writer.SetMetadata(b.commitID, b.driver.DriverID, sample.DriverVersion(),
		b.driver.SecondaryDriverID(), sample.SecondaryDriverVersion(), client.DriverDetails(sample))
}

// executePhase runs a single phase and writes its NDJSON record.
func (b *Benchmark) executePhase(phase config.PhaseConfig) (string, error) {
	depth := phase.EffectivePipelineDepth()
	b.logger.Printf("=== phase %s (%s): connections=%d pipeline_depth=%d ===",
		phase.ID, phase.Description, phase.Connections, depth)

	merged := metrics.NewCollector()
	status, activeConns, socketsPerClient, err := b.runPhase(phase, merged)

	if werr := b.writer.WritePhaseResults(phase.ID, status, activeConns, depth, socketsPerClient, merged); werr != nil {
		return status, fmt.Errorf("write phase results: %w", werr)
	}

	dur := float64(merged.DurationMillis()) / 1000.0
	rps := 0.0
	if dur > 0 {
		rps = float64(merged.TotalRequests()) / dur
	}
	b.logger.Printf("=== phase %s: %s | %.1fs | requests=%d errors=%d | %.0f req/s ===",
		phase.ID, status, dur, merged.TotalRequests(), merged.TotalErrors(), rps)
	return status, err
}

// runPhase creates the connections, warms up, runs the workload, and merges the
// per-worker collectors. It returns the phase status, the number of connections
// that actually did work (activeConns, which can be below phase.Connections when
// an rps limit is lower than the connection count), and the sockets-per-client
// count reported by the driver. Reporting activeConns keeps the row's
// connections/total_sockets honest with the sockets actually opened.
func (b *Benchmark) runPhase(phase config.PhaseConfig, merged *metrics.Collector) (string, int, int, error) {
	depth := phase.EffectivePipelineDepth()
	workerCount := phase.Connections
	if workerCount < 1 {
		workerCount = 1
	}
	activeConns := b.activeConnectionCount(phase, workerCount)

	// --- create connections (outside the measured window) -------------------
	// A cps_limit throttles connection creation.
	cpsLimiter := NewRateLimiter(phase.CpsLimit)
	clients := make([]client.BenchmarkClient, 0, activeConns)
	closeAll := func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}
	for i := 0; i < activeConns; i++ {
		cpsLimiter.Acquire()
		c, err := client.CreateAndConnect(b.host, b.port, b.driver, depth)
		if err != nil {
			closeAll()
			// A connection failure still needs a real (empty) window so the row
			// is not a null-timestamp schema violation.
			merged.Start()
			merged.Stop()
			return StatusError, activeConns, 1, fmt.Errorf("create connection %d: %w", i, err)
		}
		clients = append(clients, c)
	}
	defer closeAll()

	socketsPerClient := 1
	if len(clients) > 0 {
		socketsPerClient = client.SocketsPerClient(clients[0])
	}

	// --- warmup (before the clock): exactly warmup_requests PINGs per client --
	if err := b.runWarmup(clients, phase.EffectiveWarmupRequests()); err != nil {
		merged.Start()
		merged.Stop()
		return StatusError, activeConns, socketsPerClient, err
	}

	// --- workload -----------------------------------------------------------
	status := b.runWorkload(phase, clients, activeConns, workerCount, merged)
	return status, activeConns, socketsPerClient, nil
}

// runWorkload spawns depth worker goroutines per connection, all sharing a
// single request budget, and merges their collectors. Returns the phase status.
func (b *Benchmark) runWorkload(phase config.PhaseConfig, clients []client.BenchmarkClient,
	activeConns, workerCount int, merged *metrics.Collector) string {

	depth := phase.EffectivePipelineDepth()
	keyGenSet := NewKeyGenSet(phase.Keyspace)
	cmds, err := command.NewAll(phase.Commands)
	if err != nil {
		merged.Start()
		merged.Stop()
		b.logger.Printf("command build error: %v", err)
		return StatusError
	}

	// Shared request budget: one atomic counter claimed one request at a time.
	var remaining atomic.Int64
	if phase.Completion.IsRequestBased() {
		remaining.Store(int64(phase.Completion.Requests))
	} else {
		remaining.Store(-1)
	}
	var deadline time.Time
	if phase.Completion.IsDurationBased() {
		deadline = time.Now().Add(time.Duration(phase.Completion.Seconds) * time.Second)
	}

	merger := metrics.NewSafeMerger(merged)
	var wg sync.WaitGroup
	var workerPanics atomic.Int64

	// Interval reporting: each worker additionally records into its own
	// mutex-guarded window collector, which the reporter swaps out every N
	// seconds. The lock is per worker, so it is uncontended except for the
	// instant of the swap. Disabled → windows is nil and the hot path is unchanged.
	var reporter *intervalReporter
	if phase.IntervalReportingEnabled() {
		reporter = b.newIntervalReporter(phase, activeConns, activeConns*depth)
	}

	for connIdx := 0; connIdx < activeConns; connIdx++ {
		// One key generator + selector per CONNECTION, shared by that
		// connection's pipeline_depth workers, so the key stream stays a property
		// of the connection regardless of depth.
		keyGen := keyGenSet.ForWorker(connIdx)
		selector := NewCommandSelector(cmds)
		// One rate limiter per connection, shared across its depth slots, so the
		// connection's rps share is enforced across all its in-flight requests
		// (not multiplied by depth).
		connRps := b.connectionRps(phase, workerCount, connIdx)
		limiter := NewRateLimiter(connRps)
		c := clients[connIdx]

		for slot := 0; slot < depth; slot++ {
			var win *windowSlot
			if reporter != nil {
				win = reporter.slots[connIdx*depth+slot]
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				coll := metrics.NewCollector()
				coll.Start()
				// Close the collector's window and merge it even if the worker
				// panics, so a panicking worker still contributes a valid
				// (non-null) window rather than leaving the phase with a
				// null-timestamp row. Registered before the recover defer so it
				// runs after recovery, with the window intact.
				defer func() {
					coll.Stop()
					merger.Merge(coll)
				}()
				// A panic in a worker (not an ordinary command error) is a true
				// worker failure: recover it, record it, and mark the phase ERROR.
				defer func() {
					if r := recover(); r != nil {
						workerPanics.Add(1)
						b.logger.Printf("worker panic: %v", r)
					}
				}()
				b.pipelineWorker(c, keyGen, selector, limiter, &remaining, deadline,
					phase.Completion.IsDurationBased(), coll, win)
			}()
		}
	}
	if reporter != nil {
		reporter.start()
	}
	wg.Wait()
	if reporter != nil {
		reporter.finish()
	}

	if b.interrupted.Load() {
		return StatusInterrupted
	}
	// A worker that panicked is a genuine failure of the run.
	if workerPanics.Load() > 0 {
		return StatusError
	}
	// Ordinary command errors are recorded, not fatal — matching the reference
	// engines. A phase is only ERROR if EVERY request failed (no usable data).
	if merged.TotalRequests() > 0 && merged.TotalRequests() == merged.TotalErrors() {
		b.logger.Printf("all %d requests failed; reporting phase as ERROR", merged.TotalRequests())
		return StatusError
	}
	return StatusCompleted
}

// pipelineWorker is one in-flight slot on a shared connection. It draws from the
// shared budget (or runs until the deadline) and records into its own collector.
// Ordinary command failures are recorded and the loop continues (with backoff on
// sustained failure); they do not by themselves fail the phase.
func (b *Benchmark) pipelineWorker(c client.BenchmarkClient, keyGen *KeyGenerator,
	selector *CommandSelector, limiter *RateLimiter, remaining *atomic.Int64,
	deadline time.Time, durationBased bool, coll *metrics.Collector, win *windowSlot) {

	consecutiveFailures := 0
	for {
		if b.interrupted.Load() {
			return
		}
		if durationBased {
			if !time.Now().Before(deadline) {
				return
			}
		} else if remaining.Add(-1) < 0 {
			return
		}

		limiter.Acquire()
		cmd := selector.Select()
		res := cmd.Execute(c, keyGen)
		coll.Record(res)
		if win != nil {
			win.record(res)
		}

		if res.Success {
			consecutiveFailures = 0
			continue
		}
		// Back off a permanently-failing connection so it cannot spin at full CPU
		// inflating the error count. The error is recorded above regardless.
		consecutiveFailures++
		if consecutiveFailures >= failureBackoffAfter {
			d := time.Duration(consecutiveFailures-failureBackoffAfter+1) * time.Millisecond
			if d > maxFailureBackoff {
				d = maxFailureBackoff
			}
			time.Sleep(d)
		}
	}
}

const (
	failureBackoffAfter = 5
	maxFailureBackoff   = 100 * time.Millisecond
)

// runWarmup issues exactly warmup_requests PINGs per client (not scaled by depth
// or split across connections), matching Java/Ruby/Python. A failed warmup PING
// fails the phase fast rather than running a whole phase that records only errors.
func (b *Benchmark) runWarmup(clients []client.BenchmarkClient, warmupRequests int) error {
	if warmupRequests <= 0 {
		return nil
	}
	var wg sync.WaitGroup
	var firstErr atomic.Value // error
	for _, c := range clients {
		wg.Add(1)
		go func(c client.BenchmarkClient) {
			defer wg.Done()
			for i := 0; i < warmupRequests; i++ {
				if r := c.Ping(); !r.Success() {
					firstErr.CompareAndSwap(nil, fmt.Errorf("warmup PING failed: %v", r.Err))
					return
				}
			}
		}(c)
	}
	wg.Wait()
	if v := firstErr.Load(); v != nil {
		return v.(error)
	}
	return nil
}

// activeConnectionCount caps the number of connections that do work. When an rps
// limit is lower than the connection count, only rps_limit connections run,
// otherwise flooring the per-connection share would overshoot the target.
func (b *Benchmark) activeConnectionCount(phase config.PhaseConfig, workerCount int) int {
	if phase.HasRpsLimit() && phase.RpsLimit < workerCount {
		if phase.RpsLimit < 1 {
			return 1
		}
		return phase.RpsLimit
	}
	return workerCount
}

// connectionRps returns a connection's rps share, or -1 for unlimited. The phase
// limit is split across active connections with the remainder distributed.
func (b *Benchmark) connectionRps(phase config.PhaseConfig, workerCount, connIdx int) int {
	if !phase.HasRpsLimit() {
		return -1
	}
	return splitShare(phase.RpsLimit, workerCount, connIdx)
}

// splitShare distributes total across workerCount, giving the first
// (total % workerCount) workers one extra. The shares sum exactly to total.
func splitShare(total, workerCount, workerIndex int) int {
	if workerCount <= 0 {
		return 0
	}
	base := total / workerCount
	if workerIndex < total%workerCount {
		base++
	}
	return base
}
