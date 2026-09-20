#!/usr/bin/env bash
#
# run-remote.sh — the on-instance benchmark job runner (AWS side).
#
# Invoked by the CloudFormation UserData after provision.sh has installed the
# toolchain. This script owns the AWS-specific run lifecycle:
#
#   1. schedule a hard runtime cap (`shutdown -h +N`) so a hung run can't bill
#      indefinitely;
#   2. start the Valkey server and confirm PING;
#   3. send the CreationPolicy SUCCESS signal (cfn-signal) — this is what
#      unblocks `aws cloudformation deploy`, gating on a genuinely healthy box;
#   4. run the matrix sweep into runs/<job_id>/results/;
#   5. generate the interactive HTML report;
#   6. assemble run-metadata.json;
#   7. upload the whole bundle to s3://<bucket>/runs/<job_id>/;
#   8. `shutdown -h now` (→ self-TERMINATE, per the instance's
#      InstanceInitiatedShutdownBehavior).
#
# A `trap ... EXIT` guarantees that steps 6-8 (upload results + the per-cell
# manifest, then terminate) happen EVEN WHEN THE SWEEP FAILS — a failed run
# leaves a diagnosable bundle in S3, not a hung, billing box.
#
# All AWS/IMDS/S3/cfn-signal calls live here (never in provision.sh). Region is
# pinned via AWS_REGION (us-east-1), never inherited from the ambient shell.
#
# Inputs (exported by UserData):
#   JOB_ID, S3_BUCKET, MATRIX_NAME, REPO_TAG, RUNTIME_CAP_MINUTES,
#   AWS_REGION, STACK_NAME, CFN_SIGNAL_RESOURCE, CFN_INSTANCE_ID, REPO_DIR

set -uo pipefail

log() { printf '[run-remote] %s\n' "$*"; }

# ─────────────────────────────────────────────────────────────────────────────
# Inputs & derived paths
# ─────────────────────────────────────────────────────────────────────────────
REPO_DIR="${REPO_DIR:-/opt/resp-bench}"
JOB_ID="${JOB_ID:?JOB_ID is required}"
S3_BUCKET="${S3_BUCKET:-valkey-glide-resp-bench}"
MATRIX_NAME="${MATRIX_NAME:-valkey-glide-basic-multiclients}"
REPO_TAG="${REPO_TAG:-unknown}"
RUNTIME_CAP_MINUTES="${RUNTIME_CAP_MINUTES:-180}"
AWS_REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="${STACK_NAME:-}"
CFN_SIGNAL_RESOURCE="${CFN_SIGNAL_RESOURCE:-BenchmarkInstance}"
CFN_INSTANCE_ID="${CFN_INSTANCE_ID:-}"

MATRIX_PATH="configs/matrices/${MATRIX_NAME}.json"
STAGE_DIR="/opt/resp-bench-runs/${JOB_ID}"
RESULTS_DIR="${STAGE_DIR}/results"
GRAPHS_DIR="${STAGE_DIR}/graphs"
LOGS_DIR="${STAGE_DIR}/logs"
METADATA_FILE="${STAGE_DIR}/run-metadata.json"
REPORT_FILE="${STAGE_DIR}/report.html"
S3_PREFIX="s3://${S3_BUCKET}/runs/${JOB_ID}/"

START_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
SIGNALLED=0
UPLOADED=0
SWEEP_RC=""

mkdir -p "${RESULTS_DIR}" "${GRAPHS_DIR}" "${LOGS_DIR}"

# ─────────────────────────────────────────────────────────────────────────────
# IMDSv2 helpers (AWS-specific — correct place for them)
# ─────────────────────────────────────────────────────────────────────────────
imds() {
  # imds <metadata-path> -> value (empty on failure)
  local token
  token="$(curl -sS -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 300" 2>/dev/null || true)"
  curl -sS -H "X-aws-ec2-metadata-token: ${token}" \
    "http://169.254.169.254/latest/meta-data/${1}" 2>/dev/null || true
}

INSTANCE_ID="${CFN_INSTANCE_ID:-$(imds instance-id)}"
INSTANCE_TYPE="$(imds instance-type)"
AVAILABILITY_ZONE="$(imds placement/availability-zone)"
AMI_ID="$(imds ami-id)"

# ─────────────────────────────────────────────────────────────────────────────
# cfn-signal — unblocks the CreationPolicy in `deploy`
# ─────────────────────────────────────────────────────────────────────────────
cfn_signal() {
  # cfn_signal SUCCESS|FAILURE
  [ -z "${STACK_NAME}" ] && { log "no STACK_NAME; skipping cfn-signal $1"; return 0; }
  log "cfn-signal $1"
  aws cloudformation signal-resource \
    --region "${AWS_REGION}" \
    --stack-name "${STACK_NAME}" \
    --logical-resource-id "${CFN_SIGNAL_RESOURCE}" \
    --unique-id "${INSTANCE_ID}" \
    --status "$1" || log "WARNING: cfn-signal $1 call failed"
}

# ─────────────────────────────────────────────────────────────────────────────
# Upload the bundle to S3 (idempotent; safe to call more than once)
# ─────────────────────────────────────────────────────────────────────────────
upload_bundle() {
  [ "${UPLOADED}" -eq 1 ] && return 0
  # Best-effort capture of the boot/provision log for post-mortem.
  cp /var/log/cloud-init-output.log "${LOGS_DIR}/cloud-init-output.log" 2>/dev/null || true
  log "uploading bundle to ${S3_PREFIX}"
  if aws s3 cp --region "${AWS_REGION}" --recursive "${STAGE_DIR}/" "${S3_PREFIX}"; then
    UPLOADED=1
    log "upload complete"
  else
    log "WARNING: S3 upload failed"
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# EXIT trap — the safety net: upload whatever exists, then self-terminate,
# no matter how we got here (success, sweep failure, or unexpected abort).
# ─────────────────────────────────────────────────────────────────────────────
# shellcheck disable=SC2329  # invoked indirectly via `trap ... EXIT`
on_exit() {
  local rc=$?
  log "EXIT trap (rc=${rc}); ensuring results are uploaded and box terminates"
  # If we never reached the healthy-signal point, tell CFN we failed so the
  # stack rolls back promptly instead of timing out.
  if [ "${SIGNALLED}" -eq 0 ]; then
    cfn_signal FAILURE
    SIGNALLED=1
  fi
  # Only synthesize "aborted" metadata if the normal path never uploaded a
  # bundle (with its honest succeeded/failed status). Otherwise leave the good
  # bundle untouched.
  if [ "${UPLOADED}" -eq 0 ]; then
    write_metadata "aborted"
    upload_bundle
  fi
  log "self-terminating via 'shutdown -h now'"
  shutdown -h now || sudo shutdown -h now || true
}
trap on_exit EXIT

# ─────────────────────────────────────────────────────────────────────────────
# run-metadata.json assembler
# ─────────────────────────────────────────────────────────────────────────────
write_metadata() {
  # write_metadata <status>
  local status="$1" end_ts git_sha valkey_version
  end_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  git_sha="$(git -C "${REPO_DIR}" rev-parse HEAD 2>/dev/null || echo unknown)"
  valkey_version="$("${REPO_DIR}/work/valkey/bin/valkey-server" --version 2>/dev/null | head -n1 || echo unknown)"

  if command -v jq >/dev/null 2>&1; then
    jq -n \
      --arg job_id "${JOB_ID}" \
      --arg status "${status}" \
      --arg matrix "${MATRIX_NAME}" \
      --arg repo_tag "${REPO_TAG}" \
      --arg git_sha "${git_sha}" \
      --arg region "${AWS_REGION}" \
      --arg instance_id "${INSTANCE_ID}" \
      --arg instance_type "${INSTANCE_TYPE}" \
      --arg az "${AVAILABILITY_ZONE}" \
      --arg ami_id "${AMI_ID}" \
      --arg valkey_version "${valkey_version}" \
      --arg sweep_rc "${SWEEP_RC}" \
      --arg start "${START_TS}" \
      --arg end "${end_ts}" \
      '{
        job_id: $job_id,
        status: $status,
        matrix: $matrix,
        repo_tag: $repo_tag,
        git_sha: $git_sha,
        sweep_exit_code: $sweep_rc,
        server: { valkey_version: $valkey_version },
        aws: { region: $region, instance_id: $instance_id, instance_type: $instance_type, availability_zone: $az, ami_id: $ami_id },
        timing: { start: $start, end: $end }
      }' > "${METADATA_FILE}" 2>/dev/null || true
  else
    # Fallback if jq is somehow unavailable.
    printf '{"job_id":"%s","status":"%s","matrix":"%s","repo_tag":"%s","git_sha":"%s","sweep_exit_code":"%s","instance_type":"%s","availability_zone":"%s","ami_id":"%s","region":"%s","start":"%s","end":"%s"}\n' \
      "${JOB_ID}" "${status}" "${MATRIX_NAME}" "${REPO_TAG}" "${git_sha}" "${SWEEP_RC}" \
      "${INSTANCE_TYPE}" "${AVAILABILITY_ZONE}" "${AMI_ID}" "${AWS_REGION}" "${START_TS}" "${end_ts}" \
      > "${METADATA_FILE}"
  fi
}

# ═════════════════════════════════════════════════════════════════════════════
# Main flow
# ═════════════════════════════════════════════════════════════════════════════
cd "${REPO_DIR}" || exit 1

# Make the provisioned toolchain (e.g. dotnet on PATH) visible here.
if [ -f "${REPO_DIR}/.resp-bench-env" ]; then
  # shellcheck source=/dev/null
  . "${REPO_DIR}/.resp-bench-env"
fi

# (1) Hard runtime cap — self-terminate after N minutes no matter what.
log "scheduling runtime cap: shutdown -h +${RUNTIME_CAP_MINUTES}"
shutdown -h "+${RUNTIME_CAP_MINUTES}" "resp-bench runtime cap reached" \
  || sudo shutdown -h "+${RUNTIME_CAP_MINUTES}" || log "WARNING: could not schedule runtime cap"

# (2) Start the server and confirm PING.
log "starting Valkey server"
make server-standalone-start
CLI="${REPO_DIR}/work/valkey/bin/valkey-cli"
log "probing server readiness (PING)"
PING_OK=0
for _ in $(seq 1 30); do
  if [ -x "${CLI}" ] && [ "$("${CLI}" -h 127.0.0.1 -p 6379 ping 2>/dev/null)" = "PONG" ]; then
    PING_OK=1
    break
  fi
  sleep 1
done
if [ "${PING_OK}" -ne 1 ]; then
  log "ERROR: server did not become ready; signalling FAILURE and aborting"
  cfn_signal FAILURE
  SIGNALLED=1
  exit 1   # → EXIT trap uploads diagnostics and terminates
fi
log "server is up (PONG)"

# (3) Provisioning + server are healthy — unblock `deploy`.
cfn_signal SUCCESS
SIGNALLED=1

# (4) Run the sweep. Use the Makefile contract (stable), organizing output into
#     the job-specific results dir ourselves (the runner on this ref has no
#     --run-id). We do NOT abort on failure: the trap must still upload + the
#     honest exit code is recorded in the metadata.
log "running sweep: matrix=${MATRIX_PATH} -> ${RESULTS_DIR}"
make benchmark-matrix \
  MATRIX="${MATRIX_PATH}" \
  OUTPUT_DIR="${RESULTS_DIR}" \
  SERVER_HOST=127.0.0.1
SWEEP_RC=$?
log "sweep finished with exit code ${SWEEP_RC}"

# (5) Generate the interactive HTML report (best effort).
log "generating report"
if make benchmark-matrix-graphs OUTPUT_DIR="${RESULTS_DIR}" GRAPHS_DIR="${GRAPHS_DIR}"; then
  if [ -f "${GRAPHS_DIR}/scalability_and_delta.html" ]; then
    cp "${GRAPHS_DIR}/scalability_and_delta.html" "${REPORT_FILE}"
  fi
else
  log "WARNING: report generation failed"
fi

# (5b) Memory chart — RSS over time per series, plus a flat/rising verdict.
#      Separate from the step above because generate_interactive_graphs.py reads
#      only cpu_percent out of the .system.ndjson files and ignores the
#      memory_rss_bytes the monitor already collects. Best effort: a soak whose
#      throughput numbers are good must still upload even if this chart fails.
log "generating memory chart"
if python scripts/plot_memory.py "${RESULTS_DIR}" \
     --output "${GRAPHS_DIR}/memory.html" \
     --title "Memory usage over time — ${MATRIX_NAME} (${JOB_ID})"; then
  log "memory chart written to ${GRAPHS_DIR}/memory.html"
else
  log "WARNING: memory chart generation failed"
fi

# (6) Metadata with the honest final status.
if [ "${SWEEP_RC}" -eq 0 ]; then
  write_metadata "succeeded"
else
  write_metadata "failed"
fi

# (7) Upload the bundle.
upload_bundle

log "run complete for ${JOB_ID}; results at ${S3_PREFIX}"
# (8) Normal completion: the EXIT trap performs the final upload check and
#     `shutdown -h now`. Exit with the sweep's honest status.
exit "${SWEEP_RC}"
