package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/resp-bench/go/internal/config"
)

// GoRedisClient is the go-redis (github.com/redis/go-redis/v9) driver.
//
// Behavior is pinned to match the other engines' one-round-trip-per-request
// contract (see the Python redis-py driver):
//   - RESP3 (Protocol: 3)
//   - retries DISABLED (MaxRetries: -1): a silently retried failure must be
//     recorded as an error, not as a success with inflated latency.
//   - a single pooled connection per client (PoolSize: 1), matching the
//     "one in-flight request per connection" model of the other drivers.
//   - values kept as raw bytes (no decode overhead).
type GoRedisClient struct {
	rdb         *redis.Client
	ctx         context.Context
	maxInFlight int
}

// NewGoRedisClient returns an unconnected go-redis client.
func NewGoRedisClient() *GoRedisClient {
	return &GoRedisClient{ctx: context.Background(), maxInFlight: 1}
}

// SetMaxInFlight declares the pipeline depth this client will be driven at. The
// connection pool is bounded to this so pipeline_depth > 1 checks out at most
// that many sockets — keeping the client==connection(s) accounting honest.
func (c *GoRedisClient) SetMaxInFlight(depth int) {
	if depth < 1 {
		depth = 1
	}
	c.maxInFlight = depth
}

// SocketsPerClient reports the pool size (one socket per in-flight slot). go-redis
// pipelines via a connection pool, unlike a multiplexing driver.
func (c *GoRedisClient) SocketsPerClient() int { return c.maxInFlight }

// DriverDetails records the pinned protocol/retry settings and pipelining model.
func (c *GoRedisClient) DriverDetails() map[string]any {
	return map[string]any{
		"resp_protocol":   3,
		"retries":         0,
		"response_parser": "go-redis",
		"pipelining":      "connection-pool (up to pipeline_depth sockets per client)",
	}
}

// Prime materializes the whole pool before the measured window. go-redis opens a
// pooled connection lazily, so at depth N only one socket exists after Connect;
// issuing N concurrent PINGs forces all N to be created and handshaked now.
func (c *GoRedisClient) Prime() error {
	if c.maxInFlight <= 1 || c.rdb == nil {
		return nil
	}
	errs := make(chan error, c.maxInFlight)
	for i := 0; i < c.maxInFlight; i++ {
		go func() { errs <- c.rdb.Ping(c.ctx).Err() }()
	}
	var firstErr error
	for i := 0; i < c.maxInFlight; i++ {
		if e := <-errs; e != nil && firstErr == nil {
			firstErr = e
		}
	}
	return firstErr
}

// Connect opens the client and eagerly pings so failures surface here.
func (c *GoRedisClient) Connect(host string, port int, cfg config.DriverConfig) error {
	opt := &redis.Options{
		Addr:       host + ":" + itoa(port),
		Protocol:   3,             // RESP3, pinned so behavior does not depend on defaults
		MaxRetries: -1,            // disable retries: one round trip per request
		PoolSize:   c.maxInFlight, // one socket per in-flight slot (pipeline_depth)
	}

	if tlsCfg, err := buildTLSConfig(cfg); err != nil {
		return err
	} else if tlsCfg != nil {
		opt.TLSConfig = tlsCfg
	}
	if cfg.Auth != nil {
		if u, ok := cfg.Auth["username"].(string); ok {
			opt.Username = u
		}
		if p, ok := cfg.Auth["password"].(string); ok {
			opt.Password = p
		}
	}
	if cfg.CommandTimeoutMs != nil && *cfg.CommandTimeoutMs > 0 {
		d := time.Duration(*cfg.CommandTimeoutMs) * time.Millisecond
		opt.ReadTimeout = d
		opt.WriteTimeout = d
	}

	c.rdb = redis.NewClient(opt)

	// Establish the connection eagerly so a bad host/port fails at connect time
	// rather than being (mis)recorded as per-request errors.
	if err := c.rdb.Ping(c.ctx).Err(); err != nil {
		_ = c.rdb.Close()
		c.rdb = nil
		return err
	}
	return nil
}

// measure times fn and packages the outcome as a TimedResult.
func (c *GoRedisClient) measure(fn func() (any, error)) TimedResult {
	start := time.Now()
	val, err := fn()
	lat := time.Since(start).Microseconds()
	if lat < 1 {
		lat = 1
	}
	return TimedResult{Value: val, LatencyMicros: lat, Err: err}
}

// Ping executes PING.
func (c *GoRedisClient) Ping() TimedResult {
	return c.measure(func() (any, error) { return c.rdb.Ping(c.ctx).Result() })
}

// Get executes GET. A missing key (redis.Nil) is a successful empty result, not
// an error — matching the other drivers.
func (c *GoRedisClient) Get(key string) TimedResult {
	return c.measure(func() (any, error) {
		v, err := c.rdb.Get(c.ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return v, err
	})
}

// Set executes SET with no expiry.
func (c *GoRedisClient) Set(key string, value []byte) TimedResult {
	return c.measure(func() (any, error) { return c.rdb.Set(c.ctx, key, value, 0).Result() })
}

// Close releases the client.
func (c *GoRedisClient) Close() error {
	if c.rdb == nil {
		return nil
	}
	return c.rdb.Close()
}

// DriverVersion returns the go-redis module version.
func (c *GoRedisClient) DriverVersion() string {
	return moduleVersion("github.com/redis/go-redis/v9", "go-redis")
}

// SecondaryDriverVersion is unused for go-redis.
func (c *GoRedisClient) SecondaryDriverVersion() string { return "" }

// buildTLSConfig returns a *tls.Config when the driver config enables TLS, or
// (nil, nil) when TLS is off. Cert paths (ca_path/cert_path/key_path) are wired
// into RootCAs/Certificates, matching redis-py which honours all three.
func buildTLSConfig(cfg config.DriverConfig) (*tls.Config, error) {
	if !tlsEnabled(cfg) {
		return nil, nil
	}
	t := &tls.Config{}
	if v, ok := cfg.TLS["verify_hostname"].(bool); ok && !v {
		t.InsecureSkipVerify = true
	}

	if caPath, ok := cfg.TLS["ca_path"].(string); ok && caPath != "" {
		pem, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("tls: read ca_path %q: %w", caPath, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls: ca_path %q contains no valid certificates", caPath)
		}
		t.RootCAs = pool
	}

	certPath, _ := cfg.TLS["cert_path"].(string)
	keyPath, _ := cfg.TLS["key_path"].(string)
	switch {
	case certPath != "" && keyPath != "":
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("tls: load cert_path/key_path: %w", err)
		}
		t.Certificates = []tls.Certificate{cert}
	case certPath != "" || keyPath != "":
		return nil, fmt.Errorf("tls: cert_path and key_path must be set together")
	}

	return t, nil
}
