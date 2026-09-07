#!/usr/bin/env bash
# End-to-end proof that this connector is installable and runnable the way a stranger would use it:
# publish (self-contained, this machine's RID) -> pack -> local folder feed -> `pz restore` resolves
# it from that feed and materializes this RID's binary -> `pz run` moves real data through both
# directions (delta-rs write, delta_scan read) -> `pz retry` reuses the staged extraction ->
# `pz connector test` runs the PCP conformance vectors against the materialized package ->
# `pz mcp` spawns the connector once per tool call.
#
# The connector is a separate process (PZ0360): pz spawns runtimes/<rid>/native/Pz.Connector.DeltaLake
# and talks PCP to it over a socket. The two Rust libraries delta-rs needs sit beside that binary and
# are loaded by the connector process, never by pz. What this script asserts about the materialized
# package is therefore that the binary and both libraries landed under <package>/native/ byte-for-byte
# as published -- the directory the manifest's entrypoint points at -- and that pz can spawn it.
#
# NOT hermetic, unlike pz's own verify script: the local feed carries this connector, but `pz`, the
# .NET runtime pack the self-contained publish needs, and DuckDB's `delta` extension are fetched from
# the network.
#
# Two feed mechanisms are in play and they are not the same one. `dotnet tool install` reads the
# NuGet.Config written below; `pz restore` never reads NuGet.Config at all -- its feeds come from
# --feeds, else PZ_FEEDS, else nuget.org -- so the local feed is passed to it explicitly.
#
# NEEDS ROUGHLY 1 GB OF FREE SPACE UNDER TMPDIR: a self-contained publish, the packed nupkg, a cold
# package cache and the materialized package. Where /tmp is a small tmpfs run it as
# `TMPDIR=/var/tmp scripts/verify-external-connector.sh`.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT="${ROOT_DIR}/src/Pz.Connector.DeltaLake/Pz.Connector.DeltaLake.csproj"
WORK_DIR="$(mktemp -d)"
FEED_DIR="${WORK_DIR}/feed"
TOOL_DIR="${WORK_DIR}/tool"
PROJ_DIR="${WORK_DIR}/project"
STAGE_DIR="${WORK_DIR}/pz-native/"
NUGET_CONFIG="${WORK_DIR}/nuget.config"
# PZ_VERIFY_KEEP=1 leaves the work directory behind so a failure can be inspected rather than
# reproduced from scratch; the default is to clean up.
if [[ "${PZ_VERIFY_KEEP:-0}" == "1" ]]; then
  trap 'echo "work dir kept: ${WORK_DIR}"' EXIT
else
  trap 'rm -rf "${WORK_DIR}"' EXIT
fi

mkdir -p "${FEED_DIR}" "${TOOL_DIR}"

# A private, empty package cache: pz's content-addressed cache is per-user and shared across
# projects, so reusing it would report a cache hit and measure a download of zero. Everything this
# script reports about size is therefore a cold-cache number.
export PZ_CACHE_DIR="${WORK_DIR}/cache"

echo "== Pz.Connector.DeltaLake external-connector verification =="
echo "work dir: ${WORK_DIR}"
RID="$(dotnet --info | sed -n 's/^ *RID: *//p' | head -1)"
echo "rid:      ${RID}"

echo "-- Publishing the connector for ${RID} (self-contained single file) --"
# A separate `restore -r` before `publish -r --no-restore` is required on a cold NuGet cache: the
# restore folded into `publish -r` alone does not pull the RID-specific runtime pack this
# self-contained build needs (NETSDK1112), even though the same restore run standalone does.
dotnet restore "${PROJECT}" -r "${RID}" --nologo -v quiet
dotnet publish "${PROJECT}" -c Release -r "${RID}" --no-restore -p:PzNativeStaging="${STAGE_DIR}" \
  --nologo -v quiet
STAGED="${STAGE_DIR}${RID}"
for f in Pz.Connector.DeltaLake libdelta_rs_bridge.so libdelta_kernel_ffi.so; do
  [[ -f "${STAGED}/${f}" ]] || { echo "FAIL: publish did not stage ${f} under ${STAGED}" >&2; exit 1; }
done

echo "-- Packing the connector to a local folder feed --"
# PzRuntimeIdentifiers is narrowed to the one RID this machine published: the package under test
# ships one platform on purpose, and the SDK's missing-RID warning (PZSDK002) is about releases.
dotnet pack "${PROJECT}" -c Release -o "${FEED_DIR}" -p:PzNativeStaging="${STAGE_DIR}" \
  -p:PzRuntimeIdentifiers="${RID}" --nologo -v quiet
PKG="$(find "${FEED_DIR}" -maxdepth 1 -name 'Pz.Connector.DeltaLake.*.nupkg' | head -1)"
[[ -n "${PKG}" ]] || { echo "FAIL: no nupkg produced" >&2; exit 1; }
VERSION="$(basename "${PKG}" .nupkg | sed 's/^Pz\.Connector\.DeltaLake\.//')"
echo "packed ${VERSION} ($(du -h "${PKG}" | cut -f1))"

echo "-- Asserting the generated manifest --"
# The manifest is written by running the published binary in --pz-manifest mode, so it and the PCP
# handshake come from one object. What is asserted here is what `pz restore` gates on.
MANIFEST="$(unzip -p "${PKG}" pz.connector.json)"
grep -q '"runtime": "process"' <<<"${MANIFEST}" || { echo "FAIL: manifest does not declare runtime process (PZ0360)" >&2; exit 1; }
grep -q '"name": "deltalake"' <<<"${MANIFEST}" || { echo "FAIL: manifest name is not deltalake" >&2; exit 1; }
grep -q "\"${RID}\": \"native/Pz.Connector.DeltaLake\"" <<<"${MANIFEST}" || {
  echo "FAIL: manifest has no ${RID} entrypoint" >&2; echo "${MANIFEST}" >&2; exit 1; }
grep -q '"NativeScan"' <<<"${MANIFEST}" || { echo "FAIL: manifest does not declare NativeScan" >&2; exit 1; }
# root: stays absolute (PZDL0101); a manifest that anchored the project directory would change that.
! grep -q 'projectDirectoryAnchor' <<<"${MANIFEST}" || { echo "FAIL: manifest declares a project-directory anchor" >&2; exit 1; }

echo "-- Writing a NuGet.Config listing the local feed FIRST, then nuget.org --"
# PZ_TOOL_FEED points the whole chain at a pz built from source rather than the released tool. It is
# how a fix to pz is verified BEFORE it ships. Unset, everything below is the released tool.
PZ_TOOL_FEED_ENTRY=""
if [[ -n "${PZ_TOOL_FEED:-}" ]]; then
  [[ -d "${PZ_TOOL_FEED}" ]] || { echo "FAIL: PZ_TOOL_FEED=${PZ_TOOL_FEED} is not a directory" >&2; exit 1; }
  PZ_TOOL_FEED="$(cd "${PZ_TOOL_FEED}" && pwd)"
  PZ_TOOL_FEED_ENTRY="    <add key=\"pz-build\" value=\"${PZ_TOOL_FEED}\" />"
  echo "  pz comes from ${PZ_TOOL_FEED}, not nuget.org"
fi
cat > "${NUGET_CONFIG}" <<EOC
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="local-feed" value="${FEED_DIR}" />
${PZ_TOOL_FEED_ENTRY}
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
EOC

echo "-- Installing pz as a tool --"
# --version is required for a prerelease: `dotnet tool install` takes the highest STABLE version
# otherwise, which is the released pz and not the build under test. This connector needs 0.5.1+.
dotnet tool install pz --tool-path "${TOOL_DIR}" --configfile "${NUGET_CONFIG}" \
  ${PZ_TOOL_VERSION:+--version "${PZ_TOOL_VERSION}"}
PZ="${TOOL_DIR}/pz"
[[ -x "${PZ}" ]] || { echo "FAIL: no pz shim at ${PZ}" >&2; exit 1; }
"${PZ}" --version

echo "-- Copying the sample project and pinning the packed version --"
cp -r "${ROOT_DIR}/samples/delta-roundtrip" "${PROJ_DIR}"
# The committed sample pins a tagged release. Rewrite it to what was just packed -- and fail loudly
# if the line it targets ever moves, rather than restoring a version nobody built.
sed -i "s/^    version: .*/    version: ${VERSION}/" "${PROJ_DIR}/project.yml"
grep -q "^    version: ${VERSION}$" "${PROJ_DIR}/project.yml" || {
  echo "FAIL: could not pin the connector version in project.yml" >&2; exit 1; }

# deltalake roots must be absolute: the connector declares no project-directory anchor, so the sample
# reads its root from the environment and the caller decides where the lake lives.
export DELTA_LAKE_ROOT="${PROJ_DIR}/out/lake"

echo "-- pz restore (downloads the runtime-sized package; it looks hung and is not) --"
(cd "${PROJ_DIR}" && "${PZ}" restore --feeds "${FEED_DIR}" \
  ${PZ_TOOL_FEED:+--feeds "${PZ_TOOL_FEED}"} --feeds "https://api.nuget.org/v3/index.json")

echo "-- Asserting the lock file was written --"
[[ -f "${PROJ_DIR}/pz.lock.json" ]] || { echo "FAIL: no pz.lock.json" >&2; exit 1; }

echo "-- Asserting pz materialized this RID's binary and both Rust libraries, intact --"
# pz flattens runtimes/<rid>/native/ into <package>/native/, which is where the manifest's entrypoint
# points and therefore the one directory that decides whether the connector can be spawned and
# whether the process it spawns can dlopen delta-rs. Each file is compared byte-for-byte against the
# publish output it came from: existence is not the question, intactness is.
PKG_DIR="${PROJ_DIR}/.pz/packages/Pz.Connector.DeltaLake/${VERSION}"
[[ -f "${PKG_DIR}/pz.connector.json" ]] || { echo "FAIL: no manifest at ${PKG_DIR#"${PROJ_DIR}/"}" >&2; exit 1; }
for f in Pz.Connector.DeltaLake libdelta_rs_bridge.so libdelta_kernel_ffi.so; do
  [[ -f "${PKG_DIR}/native/${f}" ]] || { echo "FAIL: ${f} was not materialized under native/" >&2; exit 1; }
  cmp -s "${PKG_DIR}/native/${f}" "${STAGED}/${f}" || {
    echo "FAIL: native/${f} differs from the published ${RID} file" >&2; exit 1; }
  echo "  native/${f} ($(stat -Lc%s "${PKG_DIR}/native/${f}") bytes, matches the ${RID} publish)"
done
# No executable-bit assertion: pz materializes the entrypoint as a plain file and sets the bit when
# it resolves the entrypoint to spawn it, so the bit is proven by the runs below, not here.
echo "  ~/.pz/cache after restore: $(du -sh "${PZ_CACHE_DIR}" | cut -f1)"
echo "  .pz/packages after restore: $(du -shL "${PROJ_DIR}/.pz/packages" | cut -f1)"

# The lake is a store, not a DAG edge: nothing connects the pipeline that writes it to the pipeline
# that reads it back, so the two are independent flows and their order is the caller's to choose.
echo "-- pz run orders_delta (seed CSV -> Delta table, strategy: merge, through the spawned connector) --"
(cd "${PROJ_DIR}" && "${PZ}" run orders_delta)

echo "-- pz run orders_report (Delta table -> CSV, through delta_scan) --"
(cd "${PROJ_DIR}" && "${PZ}" run orders_report)

echo "-- Asserting the delta table and the round-tripped report both exist --"
[[ -d "${PROJ_DIR}/out/lake/orders/_delta_log" ]] || { echo "FAIL: no _delta_log" >&2; exit 1; }
REPORT="$(find "${PROJ_DIR}/out/report" -name '*.csv' | head -1)"
[[ -n "${REPORT}" ]] || { echo "FAIL: no report csv" >&2; exit 1; }
ROWS="$(($(wc -l < "${REPORT}") - 1))"
[[ "${ROWS}" -eq 3 ]] || { echo "FAIL: report has ${ROWS} rows, expected 3" >&2; cat "${REPORT}" >&2; exit 1; }
echo "report (${ROWS} rows):"
sed 's/^/  /' "${REPORT}"

# The TIMESTAMP column, asserted rather than merely carried. pz rewrites every timestamp's Arrow
# timezone to a spelling delta-rs refuses, so a sample whose columns are all integers, strings and
# doubles runs this whole script green while the commonest column type in ETL cannot be written at
# all. The assertion is on the value that came BACK through delta_scan, so it fails if the write was
# refused, if the column was dropped, or if it round-tripped as something other than a timestamp.
grep -q 'last_placed' "${REPORT}" || {
  echo "FAIL: the report carries no last_placed column; the timestamp column did not round-trip" >&2
  cat "${REPORT}" >&2; exit 1; }
# Every group's max, as a full date AND time-of-day. Deliberately not an exact literal: the CSV
# writer renders a timestamp in the RUNNER's local zone, so pinning one string would pass in CI and
# fail on a developer's machine. What has to be true regardless of the zone is that three rows came
# back carrying a time of day.
TS_ROWS="$(grep -cE ',2026-01-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}' "${REPORT}" || true)"
[[ "${TS_ROWS}" -eq 3 ]] || {
  echo "FAIL: ${TS_ROWS} of 3 report rows carry a last_placed timestamp; the timestamp column did" >&2
  echo "      not survive the Delta round trip" >&2
  cat "${REPORT}" >&2; exit 1; }

echo "-- Asserting a second restore is byte-identical (determinism) --"
cp "${PROJ_DIR}/pz.lock.json" "${WORK_DIR}/lock.first"
(cd "${PROJ_DIR}" && "${PZ}" restore --feeds "${FEED_DIR}" --feeds "https://api.nuget.org/v3/index.json")
diff -q "${WORK_DIR}/lock.first" "${PROJ_DIR}/pz.lock.json" || {
  echo "FAIL: pz.lock.json changed on a second restore" >&2; exit 1; }

# `pz retry` is the command a user reaches for right after a failure, and it spawns the connector
# again in a fresh pz process. The failure is injected, not simulated: a regular file where the
# report sink needs a directory makes the SinkWrite fail with the Delta SourceLoad already succeeded,
# which is the shape retry has to reuse.
echo "-- Injecting a SinkWrite failure downstream of the Delta read --"
rm -rf "${PROJ_DIR}/out/report"
: > "${PROJ_DIR}/out/report"
if (cd "${PROJ_DIR}" && "${PZ}" run orders_report); then
  echo "FAIL: the injected failure did not fail the run" >&2; exit 1
fi

echo "-- pz retry: reuses the staged Delta extraction rather than spawning a re-read --"
rm -f "${PROJ_DIR}/out/report"
(cd "${PROJ_DIR}" && "${PZ}" retry)

echo "-- Asserting the retry reused the Delta SourceLoad rather than re-extracting --"
RESULTS="$(find "${PROJ_DIR}/.pz/runs" -name run_results.json -printf '%T@ %p\n' | sort -rn | head -1 | cut -d' ' -f2-)"
[[ -n "${RESULTS}" ]] || { echo "FAIL: no run_results.json from the retry" >&2; exit 1; }
# The node, not just any node: run_results.json's nodes are flat objects on one line, so the one
# named for the Delta SourceLoad is extracted first and its own provenance read. Matching
# "reused" anywhere in the file would also be satisfied by a reused seed-CSV load, which would
# prove nothing about the Delta table.
DELTA_NODE="$(grep -o '{[^{}]*"name": *"src_lake__orders"[^{}]*}' "${RESULTS}" | head -1)"
[[ -n "${DELTA_NODE}" ]] || {
  echo "FAIL: the retry's run_results.json has no src_lake__orders node at all" >&2
  echo "      (${RESULTS})" >&2
  exit 1; }
grep -q '"provenance": *"reused"' <<<"${DELTA_NODE}" || {
  echo "FAIL: the retry re-extracted from the Delta table instead of reusing the staged load" >&2
  echo "      src_lake__orders in ${RESULTS} reads: ${DELTA_NODE}" >&2
  exit 1
}
REPORT="$(find "${PROJ_DIR}/out/report" -name '*.csv' | head -1)"
[[ -n "${REPORT}" ]] || { echo "FAIL: the retry produced no report csv" >&2; exit 1; }

# The PCP conformance vectors, against the package pz materialized. A write probe only: the read
# vectors drive a partition on the universal tier, and this source is native-scan only (PZ0312), so
# every read vector would fail for a reason that is the connector's design rather than a defect.
echo "-- pz connector test against the materialized package (write probe) --"
cat > "${PROJ_DIR}/probe.yml" <<EOC
connection:
  root: ${DELTA_LAKE_ROOT}
write:
  output: conformance_probe
  mode: append
EOC
CONFORMANCE="$(cd "${PROJ_DIR}" && "${PZ}" connector test "${PKG_DIR}" --config probe.yml)"
sed 's/^/  /' <<<"${CONFORMANCE}"
! grep -q '^FAIL' <<<"${CONFORMANCE}" || { echo "FAIL: a conformance vector failed" >&2; exit 1; }
for vector in handshake commit-abort-session-rules validation-aggregation clean-exit-on-shutdown; do
  grep -q "^PASS ${vector}" <<<"${CONFORMANCE}" || { echo "FAIL: ${vector} did not PASS" >&2; exit 1; }
done

# `pz retry` is a SEPARATE PROCESS, so everything above proves a fresh-process load and nothing about
# what happens when one pz process hosts this connector more than once. That case is real: every
# `pz mcp` tool handler builds its own ConnectorHost and disposes it on the way out
# (`await using var connectorHost = host;`), so a long-lived MCP session spawns and stops the
# connector process once per call. A connector that could not be spawned a second time -- a socket
# left behind, a stale shim -- would fail exactly there.
#
# pz_entity_schema is the cheapest handler that reaches delta-rs: it opens the Delta table and
# returns "source":"fetched", i.e. GetSchemaAsync really ran through both Rust libraries. Three
# calls in one server process.
#
# Best-effort by design: it needs a JSON-RPC client over stdio, which bash cannot do honestly, so it
# is skipped with a note where python3 is absent rather than failing the run. Where python3 IS
# present, a crash or a wrong answer FAILS -- that would be a finding about pz's connector host.
echo "-- Spawning the connector three times from ONE pz process (pz mcp) --"
if ! command -v python3 >/dev/null 2>&1; then
  echo "  skipped: no python3 on PATH, and this probe needs a stdio JSON-RPC client"
else
  PZ="${PZ}" PROJ_DIR="${PROJ_DIR}" timeout 180 python3 - <<'PYPROBE' || {
import json, os, subprocess, threading, time

pz, proj = os.environ["PZ"], os.environ["PROJ_DIR"]
p = subprocess.Popen([pz, "mcp"], cwd=proj, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE, text=True, bufsize=1)
# Both pipes are drained concurrently. `pz mcp` parks its console on stderr, so an undrained
# stderr pipe would fill and block the server mid-session.
lines, errs = [], []
threading.Thread(target=lambda: [lines.append(l.strip()) for l in p.stdout], daemon=True).start()
threading.Thread(target=lambda: [errs.append(l) for l in p.stderr], daemon=True).start()

def send(obj):
    p.stdin.write(json.dumps(obj) + "\n")
    p.stdin.flush()

send({"jsonrpc": "2.0", "id": 1, "method": "initialize",
      "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                 "clientInfo": {"name": "respawn-probe", "version": "1"}}})
time.sleep(3)
send({"jsonrpc": "2.0", "method": "notifications/initialized"})
for call_id in (2, 3, 4):
    send({"jsonrpc": "2.0", "id": call_id, "method": "tools/call",
          "params": {"name": "pz_entity_schema",
                     "arguments": {"connection": "lake", "entity": "orders"}}})
    time.sleep(6)

alive = p.poll() is None
fetched = set()
for line in lines:
    try:
        msg = json.loads(line)
    except ValueError:
        continue
    if msg.get("id") not in (2, 3, 4):
        continue
    # The tool's payload is a JSON STRING inside the MCP content block, so it has to be parsed
    # rather than substring-matched -- json.dumps of the envelope escapes every quote in it, and a
    # naive `'"source":"fetched"' in dumps(...)` never matches even on a perfectly good response.
    try:
        payload = json.loads(msg["result"]["content"][0]["text"])
    except (KeyError, IndexError, TypeError, ValueError):
        continue
    if payload.get("ok") and payload.get("result", {}).get("source") == "fetched":
        fetched.add(msg["id"])
p.kill()

if not alive:
    print("  FAIL: the pz mcp process died while respawning the connector")
    print("  stderr:", "".join(errs)[-2000:])
    raise SystemExit(1)
if fetched != {2, 3, 4}:
    print(f"  FAIL: only calls {sorted(fetched)} opened the Delta table; expected 2, 3 and 4")
    print("  stderr:", "".join(errs)[-2000:])
    raise SystemExit(1)
print("  three connector hosts built and disposed in one pz process; each spawned the connector")
print("  afresh and read the Delta schema through both Rust libraries.")
PYPROBE
    echo "FAIL: the repeated-spawn probe failed (see above)" >&2; exit 1; }
fi

echo
echo "-- What the package costs (cold cache, measured, not predicted) --"
# One number is the download and it is the nupkg: pz fetches the whole package, every RID it ships,
# and materializes only this host's. A release carries four RIDs; the package under test carries one,
# so the release download is roughly four times the nupkg line below.
echo "  this connector's nupkg (${RID} only):   $(du -h "${PKG}" | cut -f1)  <- the download, this RID"
echo "  pz package cache after restore:          $(du -sh "${PZ_CACHE_DIR}" | cut -f1)"
echo "  .pz/packages after restore:              $(du -shL "${PROJ_DIR}/.pz/packages" | cut -f1)"
echo "  the connector binary alone:              $(du -h "${PKG_DIR}/native/Pz.Connector.DeltaLake" | cut -f1)"
echo "  the ${RID} Rust pair alone:         $(du -ch "${PKG_DIR}"/native/libdelta_* | tail -1 | cut -f1)"

echo
echo "PASS: the external-connector path works end to end."
