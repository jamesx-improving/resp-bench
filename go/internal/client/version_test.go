package client

import (
	"runtime/debug"
	"strings"
	"testing"
)

func TestModuleVersionReadsLinkedDependency(t *testing.T) {
	// `go test` binaries carry no dependency list in their build info (the real
	// engine binary does — check with `go version -m bin/resp-bench`).
	if bi, ok := debug.ReadBuildInfo(); !ok || len(bi.Deps) == 0 {
		t.Skip("no dependency build info in this binary")
	}
	got := moduleVersion("github.com/redis/go-redis/v9", "go-redis")
	if !strings.HasPrefix(got, "go-redis/v9.") {
		t.Fatalf("moduleVersion(go-redis) = %q, want go-redis/v9.x from build info", got)
	}
	got = moduleVersion("github.com/valkey-io/valkey-glide/go/v2", "valkey-glide-go")
	if !strings.HasPrefix(got, "valkey-glide-go/v2.") {
		t.Fatalf("moduleVersion(glide) = %q, want valkey-glide-go/v2.x from build info", got)
	}
	t.Logf("glide=%s", got)
}

func TestModuleVersionUnknownModule(t *testing.T) {
	if got := moduleVersion("example.com/not/a/dep", "x"); got != "x/unknown" {
		t.Fatalf("got %q, want x/unknown", got)
	}
}
