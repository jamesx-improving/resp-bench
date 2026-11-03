package client

import (
	"context"
	"sync"
	"time"

	"github.com/resp-bench/go/internal/config"
	glide "github.com/valkey-io/valkey-glide/go/v2"
	glideconfig "github.com/valkey-io/valkey-glide/go/v2/config"
)

// glideOtelOnce serializes the very first GLIDE command per process.
//
// GLIDE v2.5.3's GetOtelInstance() lazily initializes a package-global singleton
// without synchronization (opentelemetry.go: `if otelInstance == nil { ... }`),
// which every command execution touches. Concurrent first use — multiple clients
// or pipeline_depth > 1 — therefore races on that global. We cannot fix the
// library, but forcing one command to run under a sync.Once makes the global's
// single write happen single-threaded, after which all concurrent callers only
// read the already-set value. See go/README.md "Known issues".
var glideOtelOnce sync.Once

// GlideClient is the Valkey GLIDE Go driver (github.com/valkey-io/valkey-glide/go/v2).
//
// GLIDE multiplexes all requests over a single socket, so pipeline_depth > 1 is
// satisfied natively: concurrent calls on one client put that many requests on
// the wire without opening extra connections. SocketsPerClient is therefore 1
// at any depth and Prime is a no-op.
type GlideClient struct {
	client *glide.Client
	ctx    context.Context
}

// NewGlideClient returns an unconnected GLIDE client.
func NewGlideClient() *GlideClient {
	return &GlideClient{ctx: context.Background()}
}

// SetMaxInFlight is a no-op: GLIDE multiplexes, so depth needs no pool sizing.
func (c *GlideClient) SetMaxInFlight(int) {}

// Prime is a no-op: a multiplexing client holds one socket regardless of depth.
func (c *GlideClient) Prime() error { return nil }

// SocketsPerClient is always 1 for the multiplexing GLIDE client.
func (c *GlideClient) SocketsPerClient() int { return 1 }

// DriverDetails records that GLIDE multiplexes over one socket and parses in Rust.
func (c *GlideClient) DriverDetails() map[string]any {
	return map[string]any{
		"resp_protocol":   3,
		"response_parser": "glide-rust",
		"retries":         0,
		"pipelining":      "multiplexed (1 socket per client)",
	}
}

// Connect builds the GLIDE client and connects (GLIDE connects on NewClient).
func (c *GlideClient) Connect(host string, port int, cfg config.DriverConfig) error {
	conf := glideconfig.NewClientConfiguration().
		WithAddress(&glideconfig.NodeAddress{Host: host, Port: port})

	if tlsEnabled(cfg) {
		conf = conf.WithUseTLS(true)
	}
	if cfg.Auth != nil {
		user, _ := cfg.Auth["username"].(string)
		pass, _ := cfg.Auth["password"].(string)
		if user != "" || pass != "" {
			conf = conf.WithCredentials(glideconfig.NewServerCredentials(user, pass))
		}
	}
	if cfg.CommandTimeoutMs != nil && *cfg.CommandTimeoutMs > 0 {
		conf = conf.WithRequestTimeout(time.Duration(*cfg.CommandTimeoutMs) * time.Millisecond)
	}

	cl, err := glide.NewClient(conf)
	if err != nil {
		return err
	}
	c.client = cl

	// Force GLIDE's racy lazy OTel singleton to initialize once, single-threaded,
	// before any concurrent command use (warmup goroutines / pipeline_depth slots).
	glideOtelOnce.Do(func() {
		_, _ = cl.Ping(c.ctx)
	})
	return nil
}

// measure times fn and packages the outcome as a TimedResult.
func (c *GlideClient) measure(fn func() (any, error)) TimedResult {
	start := time.Now()
	val, err := fn()
	lat := time.Since(start).Microseconds()
	if lat < 1 {
		lat = 1
	}
	return TimedResult{Value: val, LatencyMicros: lat, Err: err}
}

// Ping executes PING.
func (c *GlideClient) Ping() TimedResult {
	return c.measure(func() (any, error) { return c.client.Ping(c.ctx) })
}

// Get executes GET. A missing key is a successful empty result, not an error.
func (c *GlideClient) Get(key string) TimedResult {
	return c.measure(func() (any, error) {
		res, err := c.client.Get(c.ctx, key)
		if err != nil {
			return nil, err
		}
		// models.Result[string] is a nullable string; IsNil => key missing.
		if res.IsNil() {
			return nil, nil
		}
		return res.Value(), nil
	})
}

// Set executes SET (GLIDE takes a string value).
func (c *GlideClient) Set(key string, value []byte) TimedResult {
	return c.measure(func() (any, error) { return c.client.Set(c.ctx, key, string(value)) })
}

// Close releases the client (GLIDE's Close does not return an error).
func (c *GlideClient) Close() error {
	if c.client != nil {
		c.client.Close()
	}
	return nil
}

// DriverVersion returns the GLIDE Go module version.
func (c *GlideClient) DriverVersion() string {
	return moduleVersion("github.com/valkey-io/valkey-glide/go/v2", "valkey-glide-go")
}

// SecondaryDriverVersion is unused for GLIDE.
func (c *GlideClient) SecondaryDriverVersion() string { return "" }
