package client

import "runtime/debug"

// moduleVersion returns "<label>/<version>" for the dependency module at path,
// read from the build info the Go toolchain embeds in the binary — so the
// reported version always matches what was actually linked (go.mod pins can
// change without anyone remembering to update a hardcoded string). A replace
// directive reports the replacement's version. Falls back to "<label>/unknown"
// when build info is unavailable (e.g. some test binaries).
func moduleVersion(path, label string) string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			if d.Path != path {
				continue
			}
			if d.Replace != nil && d.Replace.Version != "" {
				return label + "/" + d.Replace.Version
			}
			if d.Version != "" {
				return label + "/" + d.Version
			}
		}
	}
	return label + "/unknown"
}
