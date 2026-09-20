#!/usr/bin/env bash
#
# provision.sh — cloud-agnostic toolchain installer for resp-bench.
#
# Installs every language toolchain the benchmark engines need, builds the
# Valkey server, and warms the per-engine build caches so that a subsequent
# sweep does no first-time compilation.
#
# DESIGN CONTRACT (keep this true — it is what makes the recipe reusable):
#   * NO cloud-specific calls live here. No AWS CLI, no CloudFormation, no S3,
#     no IMDS/metadata reads, no cfn-signal. All of that belongs in the
#     per-cloud wrapper under infra/aws/ (see infra/README.md). This script
#     only knows how to turn a stock Linux box into a machine that can run the
#     resp-bench matrix, so a future GCP/Azure control plane can reuse it
#     verbatim.
#   * Inputs arrive via environment variables (see below), never via cloud
#     metadata.
#   * Every block is idempotent: re-running the script is safe and cheap.
#   * The per-language blocks are ADDITIVE and clearly delimited. Adding a new
#     engine (Node.js, Go, PHP, ...) is a small, self-contained edit — copy an
#     existing "=== LANGUAGE: ... ===" block and adjust it.
#
# Environment variables (all optional; sensible defaults shown):
#   REPO_DIR       Path to the checked-out resp-bench repo. Default: the repo
#                  that contains this script (../ relative to infra/).
#   DOTNET_ROOT    Where to install the .NET SDK. Default: /usr/local/dotnet
#                  (falls back to $HOME/.dotnet if that is not writable).
#   DOTNET_CHANNEL .NET SDK channel to install. Default: 10.0
#   SKIP_SERVER_BUILD  If "1", skip compiling the Valkey server.
#   SKIP_WARM_CACHES   If "1", skip the throwaway engine builds.
#
# On success the script writes "$REPO_DIR/.resp-bench-env", a small snippet
# that exports the PATH/DOTNET_ROOT additions. Callers that run in a *separate*
# shell (e.g. the on-instance run script) should `source` that file so they see
# the installed toolchains. The script also drops the same snippet into
# /etc/profile.d/ when that directory is writable, for interactive logins.

set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap: locate the repo and set up helpers
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
DOTNET_CHANNEL="${DOTNET_CHANNEL:-10.0}"

log() { printf '[provision] %s\n' "$*"; }
err() { printf '[provision] ERROR: %s\n' "$*" >&2; }

# Run privileged commands only when we are not already root.
SUDO=""
if [ "$(id -u)" -ne 0 ]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
  else
    err "not running as root and 'sudo' is unavailable; package installs may fail"
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# Package-manager shim — detect dnf (AL2023/Fedora) vs apt (Debian/Ubuntu)
# so this script can be exercised on a non-AL2023 Linux box.
# ─────────────────────────────────────────────────────────────────────────────

PKG=""
if command -v dnf >/dev/null 2>&1; then
  PKG="dnf"
elif command -v apt-get >/dev/null 2>&1; then
  PKG="apt"
else
  err "no supported package manager found (need dnf or apt-get)"
  exit 1
fi
log "using package manager: ${PKG}"

_apt_updated=0
pkg_install() {
  # pkg_install <dnf-package-names...> "--" <apt-package-names...>
  # Splits the argument list on "--"; installs the correct set for the
  # detected package manager. Idempotent (the package managers no-op on
  # already-installed packages).
  local dnf_pkgs=() apt_pkgs=() seen_sep=0
  for arg in "$@"; do
    if [ "${arg}" = "--" ]; then seen_sep=1; continue; fi
    if [ "${seen_sep}" -eq 0 ]; then dnf_pkgs+=("${arg}"); else apt_pkgs+=("${arg}"); fi
  done

  if [ "${PKG}" = "dnf" ]; then
    ${SUDO} dnf install -y "${dnf_pkgs[@]}"
  else
    if [ "${_apt_updated}" -eq 0 ]; then
      ${SUDO} apt-get update -y
      _apt_updated=1
    fi
    DEBIAN_FRONTEND=noninteractive ${SUDO} apt-get install -y "${apt_pkgs[@]}"
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Environment snippet emitted for downstream shells (e.g. run-remote.sh)
# ─────────────────────────────────────────────────────────────────────────────

ENV_FILE="${REPO_DIR}/.resp-bench-env"
: > "${ENV_FILE}"
env_add() {
  # Append a line to the env snippet and apply it to the current shell too.
  printf '%s\n' "$1" >> "${ENV_FILE}"
  eval "$1"
}

# ─────────────────────────────────────────────────────────────────────────────
# BASE: common build tooling shared by every engine + the server build
# ─────────────────────────────────────────────────────────────────────────────

log "installing base build tooling"
if [ "${PKG}" = "dnf" ]; then
  # gcc/make/etc. for compiling the Valkey server and native gems.
  # libicu: required by the .NET runtime (dotnet aborts with an ICU error
  # otherwise, which would break the C# engine's build/run).
  pkg_install git tar gzip make gcc gcc-c++ openssl-devel pkgconf-pkg-config \
              findutils which jq ca-certificates procps-ng libicu -- \
              git tar gzip make build-essential libssl-dev pkg-config \
              findutils jq ca-certificates procps libicu-dev
else
  pkg_install git libicu-dev -- git tar gzip make build-essential libssl-dev \
              pkg-config findutils jq ca-certificates procps libicu-dev
fi

# ═════════════════════════════════════════════════════════════════════════════
# LANGUAGE: Java (JDK 21 + Maven) — engines: jedis, lettuce, valkey-glide,
#   redisson, spring-data-valkey, spring-data-redis
# NOTE: JDK 21 is mandatory — the Java engine uses virtual threads
#   (Executors.newVirtualThreadPerTaskExecutor(), a Java 21 API). JDK 17 will
#   hard-fail the build. See resp-bench Phase 0.5 (pom.xml pins java.version=21).
# ═════════════════════════════════════════════════════════════════════════════

log "LANGUAGE: Java — installing JDK 21 + Maven"
if [ "${PKG}" = "dnf" ]; then
  pkg_install java-21-amazon-corretto-devel maven -- openjdk-21-jdk maven
else
  pkg_install openjdk-21-jdk maven -- openjdk-21-jdk maven
fi

# JDK 21 may install alongside an older default JDK, so `mvn` can still pick up
# a pre-21 java — and the pom's enforce-java-21 rule then hard-fails the build.
# Pin JAVA_HOME/PATH to the 21 install explicitly and persist it via the env
# snippet so the sweep-time rebuild (run-remote.sh) uses it too.
JAVA21_HOME=""
for d in /usr/lib/jvm/*corretto*21* /usr/lib/jvm/java-21* /usr/lib/jvm/jdk-21* \
         /usr/lib/jvm/*-21-*; do
  if [ -x "${d}/bin/javac" ]; then JAVA21_HOME="${d}"; break; fi
done
[ -n "${JAVA21_HOME}" ] || { err "JDK 21 not found under /usr/lib/jvm after install"; exit 1; }
env_add "export JAVA_HOME=\"${JAVA21_HOME}\""
env_add "export PATH=\"${JAVA21_HOME}/bin:\$PATH\""
hash -r

# Maven may not be packaged in some dnf repos; ensure it exists.
if ! command -v mvn >/dev/null 2>&1; then
  pkg_install maven -- maven
fi
java -version 2>&1 | sed 's/^/[provision]   /' || true
mvn -version 2>&1 | head -n2 | sed 's/^/[provision]   /' || true

# ═════════════════════════════════════════════════════════════════════════════
# LANGUAGE: Ruby (3.2+ + bundler + dev headers) — engines: redis-rb,
#   valkey-glide-ruby. Native extensions (HDRHistogram, oj) need a compiler +
#   ruby headers. The Gemfile pulls `valkey` from a git branch, so the first
#   `bundle install` needs network access (warmed below).
# ═════════════════════════════════════════════════════════════════════════════

log "LANGUAGE: Ruby — installing Ruby + bundler + dev headers"
if ! command -v ruby >/dev/null 2>&1; then
  pkg_install ruby ruby-devel rubygems -- ruby ruby-dev
else
  # Ensure dev headers are present even if ruby itself already is.
  pkg_install ruby-devel -- ruby-dev || true
fi
if ! command -v bundle >/dev/null 2>&1 && ! ruby -e 'require "bundler"' >/dev/null 2>&1; then
  ${SUDO} gem install --no-document bundler
fi
ruby -v 2>&1 | sed 's/^/[provision]   /' || true

# ═════════════════════════════════════════════════════════════════════════════
# LANGUAGE: .NET SDK (net10.0) — engines: stackexchange-redis,
#   valkey-glide-csharp. net10.0 may not be in the distro feed, so install via
#   Microsoft's official dotnet-install.sh, which is feed-independent.
# ═════════════════════════════════════════════════════════════════════════════

log "LANGUAGE: .NET — installing SDK channel ${DOTNET_CHANNEL}"
# Choose an install dir we can actually write to.
DOTNET_ROOT="${DOTNET_ROOT:-/usr/local/dotnet}"
if ! mkdir -p "${DOTNET_ROOT}" 2>/dev/null; then
  if [ -n "${SUDO}" ] && ${SUDO} mkdir -p "${DOTNET_ROOT}" 2>/dev/null; then
    ${SUDO} chown "$(id -u):$(id -g)" "${DOTNET_ROOT}"
  else
    DOTNET_ROOT="${HOME}/.dotnet"
    mkdir -p "${DOTNET_ROOT}"
  fi
fi
if [ ! -x "${DOTNET_ROOT}/dotnet" ]; then
  curl -fsSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh
  bash /tmp/dotnet-install.sh --channel "${DOTNET_CHANNEL}" --install-dir "${DOTNET_ROOT}"
  rm -f /tmp/dotnet-install.sh
else
  log ".NET SDK already present at ${DOTNET_ROOT}"
fi
env_add "export DOTNET_ROOT=\"${DOTNET_ROOT}\""
env_add "export PATH=\"${DOTNET_ROOT}:${DOTNET_ROOT}/tools:\$PATH\""
# Opt out of first-run telemetry noise during unattended runs.
env_add "export DOTNET_CLI_TELEMETRY_OPTOUT=1"
env_add "export DOTNET_NOLOGO=1"
"${DOTNET_ROOT}/dotnet" --info 2>&1 | head -n3 | sed 's/^/[provision]   /' || true

# ═════════════════════════════════════════════════════════════════════════════
# LANGUAGE: Node.js (20+) — engines: valkey-glide-node, ioredis, iovalkey.
#   node/package.json declares "engines": {"node": ">=20"}; distro feeds often
#   ship 18 (EOL) or older, so install from NodeSource, which is version-pinned
#   and works on both dnf and apt. valkey-glide ships prebuilt native binaries
#   per platform, so no compiler is needed beyond build-essential (already
#   installed above for the server build).
# ═════════════════════════════════════════════════════════════════════════════

NODE_MAJOR="${NODE_MAJOR:-22}"
log "LANGUAGE: Node.js — installing Node ${NODE_MAJOR} + npm"
node_major_installed() {
  command -v node >/dev/null 2>&1 &&
    [ "$(node -p 'process.versions.node.split(".")[0]' 2>/dev/null || echo 0)" -ge 20 ]
}
if ! node_major_installed; then
  if [ "${PKG}" = "dnf" ]; then
    curl -fsSL "https://rpm.nodesource.com/setup_${NODE_MAJOR}.x" | ${SUDO} bash -
    pkg_install nodejs -- nodejs
  else
    curl -fsSL "https://deb.nodesource.com/setup_${NODE_MAJOR}.x" | ${SUDO} bash -
    pkg_install nodejs -- nodejs
  fi
else
  log "Node.js $(node -v) already present (>= 20)"
fi
node -v 2>&1 | sed 's/^/[provision]   /' || true
npm -v 2>&1 | sed 's/^/[provision]   /' || true

# ═════════════════════════════════════════════════════════════════════════════
# LANGUAGE: Python (3.9+ + pip) — used by the matrix orchestrator and graph
#   generator (scripts/*.py). Engine deps are installed from the repo's pinned
#   requirements files when present.
# ═════════════════════════════════════════════════════════════════════════════

log "LANGUAGE: Python 3.11 — installing (scripts/requirements.txt pins numpy/"
log "  matplotlib versions that require Python >= 3.11; AL2023 ships only 3.9)"
pkg_install python3.11 python3.11-pip -- python3.11 python3.11-venv python3-pip

# The Makefile's benchmark targets invoke bare `python`, so make 3.11 the
# default `python`/`python3`. /usr/local/bin precedes /usr/bin on PATH, so this
# shadows the stock 3.9 without disturbing the OS's own python3.
PY311="$(command -v python3.11)"
[ -n "${PY311}" ] || err "python3.11 not found after install"
${SUDO} ln -sf "${PY311}" /usr/local/bin/python3
${SUDO} ln -sf "${PY311}" /usr/local/bin/python
hash -r

python3.11 -m ensurepip --upgrade >/dev/null 2>&1 || true
if [ -f "${REPO_DIR}/scripts/requirements.txt" ]; then
  log "installing scripts/requirements.txt with Python 3.11"
  python3.11 -m pip install --user -r "${REPO_DIR}/scripts/requirements.txt"
fi

# The Python *engine* installs itself via `make python-build` (`pip install -e .`).
# There is no venv here and the system site-packages is not writable, so the
# install has to go to the user site — same `--user` the requirements install
# above already relies on. Exported through the env snippet so the sweep's own
# `make python-run` sees it, not just this shell.
env_add 'export PIP_FLAGS=--user'

python --version 2>&1 | sed 's/^/[provision]   /' || true

# ─────────────────────────────────────────────────────────────────────────────
# FUTURE ENGINES (leave room — see resp-bench plan §5.5):
#   Go (#14):      install the Go toolchain + `go mod download`
#   PHP (#15):     pkg_install php php-cli composer -- php-cli composer
# Add each as its own "LANGUAGE:" block above, mirroring the pattern.
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Make the env snippet available to interactive logins as well (best-effort).
# ─────────────────────────────────────────────────────────────────────────────
if [ -w /etc/profile.d ] || { [ -n "${SUDO}" ] && ${SUDO} test -w /etc; }; then
  if [ -n "${SUDO}" ]; then
    ${SUDO} cp "${ENV_FILE}" /etc/profile.d/resp-bench.sh 2>/dev/null || true
  else
    cp "${ENV_FILE}" /etc/profile.d/resp-bench.sh 2>/dev/null || true
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# Build the Valkey server. Without this, `make server-standalone-start`
# compiles Valkey from source on first use, inside the timed run.
# ─────────────────────────────────────────────────────────────────────────────

if [ "${SKIP_SERVER_BUILD:-0}" != "1" ]; then
  log "building Valkey server (this compiles from source once)"
  # The Makefile expresses the server binary as a file target at an absolute
  # path; building that path compiles + installs the server into work/<proj>/.
  # SERVER_PROJECT matches the Makefile default (valkey); override via env if
  # the Makefile default ever changes.
  SERVER_PROJECT="${SERVER_PROJECT:-valkey}"
  make -C "${REPO_DIR}" "${REPO_DIR}/work/${SERVER_PROJECT}/bin/${SERVER_PROJECT}-server"
else
  log "SKIP_SERVER_BUILD=1 — skipping Valkey server build"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Warm the per-engine build caches with one throwaway build each, so the sweep
# itself does no first-time compilation / dependency resolution. Sourcing the
# env file makes dotnet/etc. visible to the make sub-shells.
# ─────────────────────────────────────────────────────────────────────────────

if [ "${SKIP_WARM_CACHES:-0}" != "1" ]; then
  # shellcheck source=/dev/null
  . "${ENV_FILE}"
  # Warm-up is a cache optimization, not a correctness gate — so each build is
  # best-effort. A broken engine that the requested matrix does not use must not
  # block provisioning; the sweep's own build_engines() step rebuilds exactly
  # the engines the matrix needs and fails loudly (with diagnostics uploaded to
  # S3) if one of those is broken.
  log "warming Java build cache (mvn package)"
  make -C "${REPO_DIR}" java-build || log "WARNING: Java warm-up build failed (see above)"
  log "warming Ruby bundle (bundle install)"
  make -C "${REPO_DIR}" ruby-build || log "WARNING: Ruby warm-up build failed (see above)"
  log "warming C# build cache (dotnet build)"
  make -C "${REPO_DIR}" csharp-build || log "WARNING: C# warm-up build failed (see above)"
  log "warming Node.js deps (npm ci + tsc)"
  make -C "${REPO_DIR}" node-build || log "WARNING: Node.js warm-up build failed (see above)"
  log "warming Python engine install (pip install -e .)"
  make -C "${REPO_DIR}" python-build || log "WARNING: Python warm-up build failed (see above)"
else
  log "SKIP_WARM_CACHES=1 — skipping engine cache warm-up"
fi

log "provisioning complete"
