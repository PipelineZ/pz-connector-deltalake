#!/usr/bin/env bash
# End-to-end proof that this connector is installable and runnable the way a stranger would use it:
# pack -> local folder feed -> `pz restore` resolves it from that feed -> the connector ALC loads it
# -> `pz run` moves real data through both directions -> `pz retry` reloads it after a failure.
#
# The chain this exercises has never carried a native package before: pz selects native assets by an
# exact runtimes/<rid>/native/ prefix, materializes them, and resolves them through the connector
# ALC's unmanaged-DLL hook. The native assertion below checks the ONE directory that hook probes,
# not merely that the files exist somewhere under .pz/packages -- a library package's own native/
# directory is never on the connector's probe path, so "present on disk" and "loadable" are
# different questions and only the second one matters.
#
# NOT hermetic, unlike pz's own verify script: the local feed carries this connector, but `pz`,
# DeltaLake.Net and DuckDB's `delta` extension are fetched from the network. A local-feed-only
# configuration would fail on the first restore.
#
# Two feed mechanisms are in play and they are not the same one. `dotnet tool install` reads the
# NuGet.Config written below; `pz restore` never reads NuGet.Config at all -- its feeds come from
# --feeds, else PZ_FEEDS, else nuget.org -- so the local feed is passed to it explicitly.
#
# NEEDS ROUGHLY 1 GB OF FREE SPACE UNDER TMPDIR. The work directory holds a 222 MB download, a cold
# package cache, an SDK publish and the materialized package. Where /tmp is a small tmpfs this dies
# inside `dotnet tool install` with an opaque "Disk quota exceeded"; run it there as
# `TMPDIR=/var/tmp scripts/verify-external-connector.sh`.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$(mktemp -d)"
FEED_DIR="${WORK_DIR}/feed"
TOOL_DIR="${WORK_DIR}/tool"
PROJ_DIR="${WORK_DIR}/project"
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

echo "-- Packing the connector to a local folder feed --"
dotnet pack "${ROOT_DIR}/src/Pz.Connector.DeltaLake/Pz.Connector.DeltaLake.csproj" \
  -c Release -o "${FEED_DIR}" --nologo -v quiet
PKG="$(find "${FEED_DIR}" -maxdepth 1 -name 'Pz.Connector.DeltaLake.*.nupkg' | head -1)"
[[ -n "${PKG}" ]] || { echo "FAIL: no nupkg produced" >&2; exit 1; }
VERSION="$(basename "${PKG}" .nupkg | sed 's/^Pz\.Connector\.DeltaLake\.//')"
echo "packed ${VERSION}"

echo "-- Asserting the manifest is at the nupkg root --"
# The listing is captured before it is searched, deliberately. `unzip -l | grep -q` under `pipefail`
# reports the pipeline as failed when grep exits early on a match and unzip dies of SIGPIPE (141) --
# a FAIL on a package that does carry the manifest.
PKG_LISTING="$(unzip -l "${PKG}")"
grep -q ' pz.connector.json$' <<<"${PKG_LISTING}" || {
  echo "FAIL: pz.connector.json is not at the nupkg root; the host cannot gate protocol compatibility" >&2
  exit 1
}

echo "-- Writing a NuGet.Config listing the local feed FIRST, then nuget.org --"
cat > "${NUGET_CONFIG}" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="local-feed" value="${FEED_DIR}" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
EOF

echo "-- Installing pz as a tool --"
dotnet tool install pz --tool-path "${TOOL_DIR}" --configfile "${NUGET_CONFIG}"
PZ="${TOOL_DIR}/pz"
[[ -x "${PZ}" ]] || { echo "FAIL: no pz shim at ${PZ}" >&2; exit 1; }
"${PZ}" --version

echo "-- Copying the sample project and pinning the packed version --"
cp -r "${ROOT_DIR}/samples/delta-roundtrip" "${PROJ_DIR}"
# The committed sample pins the first tagged release, which does not exist until that tag does.
# Rewrite it to what was just packed -- and fail loudly if the line it targets ever moves, rather
# than restoring a version nobody built.
sed -i "s/^    version: .*/    version: ${VERSION}/" "${PROJ_DIR}/project.yml"
grep -q "^    version: ${VERSION}$" "${PROJ_DIR}/project.yml" || {
  echo "FAIL: could not pin the connector version in project.yml" >&2; exit 1; }

# deltalake roots must be absolute: pz gives a connector no project-directory anchor, so the sample
# reads its root from the environment and the caller decides where the lake lives.
export DELTA_LAKE_ROOT="${PROJ_DIR}/out/lake"

echo "-- pz restore (expect a ~200 MB download; it looks hung and is not) --"
(cd "${PROJ_DIR}" && "${PZ}" restore --feeds "${FEED_DIR}" --feeds "https://api.nuget.org/v3/index.json")

echo "-- Asserting the lock file was written --"
[[ -f "${PROJ_DIR}/pz.lock.json" ]] || { echo "FAIL: no pz.lock.json" >&2; exit 1; }

# Measured here, before anything below stages a byte: these are what pz alone puts on disk.
# -L because a library package is materialized as a symlink into the shared content-addressed cache,
# so the apparent size of .pz/packages is otherwise a handful of link entries.
CACHE_AFTER_RESTORE="$(du -sh "${PZ_CACHE_DIR}" | cut -f1)"
PACKAGES_AFTER_RESTORE="$(du -shL "${PROJ_DIR}/.pz/packages" | cut -f1)"

# What pz materialized has to be compared against something authoritative, and the authority on
# which asset a net10.0/<rid> application needs is the .NET SDK itself. A publish of this very
# project, restricted to this RID, resolves the nearest-TFM managed assemblies and the RID-correct
# native libraries -- exactly the job pz's resolver does at restore time. Any difference is pz
# selecting a different file from the same packages than the SDK does.
DL_VERSION="$(sed -n 's/.*PackageReference Include="DeltaLake.Net" Version="\([^"]*\)".*/\1/p' \
  "${ROOT_DIR}/src/Pz.Connector.DeltaLake/Pz.Connector.DeltaLake.csproj")"
echo "-- Resolving the same assets through the .NET SDK, to compare pz's selection against --"
SDK_ASSETS="${WORK_DIR}/sdk-assets"
dotnet publish "${ROOT_DIR}/src/Pz.Connector.DeltaLake/Pz.Connector.DeltaLake.csproj" \
  -c Release -r "${RID}" --self-contained false -o "${SDK_ASSETS}" --nologo -v quiet

echo "-- Comparing pz's materialized package against it --"
PKG_DIR="${PROJ_DIR}/.pz/packages"
LIB_DIR="${PKG_DIR}/Pz.Connector.DeltaLake/${VERSION}/lib"
PROBE_DIR="${PKG_DIR}/Pz.Connector.DeltaLake/${VERSION}/native"
BRIDGED=0

# (a) Managed assemblies. pz records lib assets in pz.lock.json as bare FILE NAMES and re-finds them
#     in the archive under the prefix "lib/" alone, so on a multi-targeted package it extracts
#     whichever target framework happens to come first in the zip.
#
#     Only DeltaLake.dll is CLASSIFIED as that defect here, and deliberately. A byte difference from
#     the SDK's copy has a second, innocent cause: pz resolves a transitive dependency
#     highest-version-wins where the SDK follows NuGet's minimum-version rule, so the two can be
#     different package VERSIONS of the same assembly. Measured: pz takes Microsoft.Data.Analysis
#     0.23.0 and extracts its lib/net8.0 build -- which IS the nearest target framework for a net10.0
#     host, i.e. correct -- while the SDK takes 0.21.1's netstandard2.0 build. DeltaLake.Net is pinned
#     to one version by this connector's own PackageReference, so for DeltaLake.dll the two sides
#     agree on the version and any difference IS the target framework.
# nullglob, and then an assertion: an unmatched glob would otherwise hand the loop one bogus
# iteration for a path that does not exist, and an empty lib/ -- a materialization that produced
# nothing at all -- has to be a failure here rather than a silently skipped comparison.
shopt -s nullglob
LIB_DLLS=("${LIB_DIR}"/*.dll)
shopt -u nullglob
[[ ${#LIB_DLLS[@]} -gt 0 ]] || {
  echo "FAIL: no managed assemblies under ${LIB_DIR#"${PROJ_DIR}/"}; the package materialized empty" >&2
  exit 1; }

BAD_DLLS=()
VERSION_NOTES=0
for dll in "${LIB_DLLS[@]}"; do
  name="$(basename "${dll}")"
  # This connector's own assembly is built here, not resolved from a feed: the packed copy and the
  # published copy are two builds of the same source and never compare equal. Nothing to learn.
  [[ "${name}" == "Pz.Connector.DeltaLake.dll" ]] && continue
  [[ -f "${SDK_ASSETS}/${name}" ]] || continue
  cmp -s "${dll}" "${SDK_ASSETS}/${name}" && continue
  if [[ "${name}" == "DeltaLake.dll" ]]; then
    echo "  UPSTREAM GAP (pz finding 21): ${name} is not the build a net10.0 host needs." >&2
    echo "    materialized: $(stat -Lc%s "${dll}") bytes (lib/net472 -- first 'lib/' entry in the zip)" >&2
    echo "    SDK-resolved: $(stat -Lc%s "${SDK_ASSETS}/${name}") bytes (lib/net9.0)" >&2
    echo "    cause: PackageMaterializer.ExtractInto resolves a lib asset by file name under the" >&2
    echo "           prefix 'lib/', discarding the nearest target framework the resolver selected." >&2
    echo "    bites as: MissingMethodException on the first delta-rs call -- the .NET Framework build" >&2
    echo "           of the vendor assembly has a different API surface than the .NET 9 one." >&2
    BAD_DLLS+=("${name}")
    BRIDGED=1
  else
    echo "  note: ${name} differs from the SDK's copy; left as pz materialized it, not a finding." >&2
    VERSION_NOTES=1
  fi
done

if [[ "${VERSION_NOTES}" == "1" ]]; then
  echo "  Those notes are pz resolving a different package VERSION than the SDK does" >&2
  echo "  (highest-version-wins against NuGet's minimum-version rule), not a different target" >&2
  echo "  framework: within the versions pz chose it extracted Microsoft.Data.Analysis 0.23.0's" >&2
  echo "  lib/net8.0 (the nearer of net8.0 and netstandard2.0) and Microsoft.ML.DataView 5.0.0's" >&2
  echo "  lib/netstandard2.0 (its only one). Both correct. Only DeltaLake.Net multi-targets in a" >&2
  echo "  way that puts a net4x build first in the zip." >&2
fi

# (b) Native libraries: the right file, on the right path, and BOTH questions asked about the ONE
#     directory that decides loadability. ConnectorLoadContext.LoadUnmanagedDll probes
#     <package>/native/ and nowhere else. Asserting only that a file of the right NAME is there
#     would green a partially-fixed pz that placed the wrong architecture on the probe path, so the
#     probe-path copy is compared byte-for-byte against the SDK-resolved one. Existence is not the
#     question; loadability is.
for lib in libdelta_rs_bridge.so libdelta_kernel_ffi.so; do
  [[ -f "${SDK_ASSETS}/${lib}" ]] || {
    echo "FAIL: DeltaLake.Net ships no ${lib} for ${RID}; this host's RID is not one it supports." >&2
    exit 1; }

  # -L because a library package is materialized as a symlink into the content-addressed cache.
  staged="$(find -L "${PKG_DIR}" -name "${lib}" | head -1)"
  [[ -n "${staged}" ]] || {
    echo "FAIL: ${lib} was not materialized anywhere under .pz/packages." >&2
    echo "      pz selects native assets by an exact runtimes/<rid>/native/ prefix with no RID-graph" >&2
    echo "      fallback; check that this host's RID matches one DeltaLake.Net ships." >&2
    exit 1; }

  if ! cmp -s "${staged}" "${SDK_ASSETS}/${lib}"; then
    echo "  UPSTREAM GAP (pz finding 20): the materialized ${lib} is not this host's build." >&2
    echo "    materialized: ${staged#"${PROJ_DIR}/"} ($(stat -Lc%s "${staged}") bytes)" >&2
    echo "    SDK-resolved: ${RID} ($(stat -Lc%s "${SDK_ASSETS}/${lib}") bytes)" >&2
    echo "    cause: PackageMaterializer.ExtractInto resolves a native asset by file name under the" >&2
    echo "           prefix 'runtimes/', discarding the RID the resolver already selected." >&2
    BRIDGED=1
  fi

  if [[ ! -f "${PROBE_DIR}/${lib}" ]]; then
    echo "  UPSTREAM GAP (pz finding 19): ${lib} is not on the connector's probe path." >&2
    echo "    probed:  .pz/packages/Pz.Connector.DeltaLake/${VERSION}/native/${lib} (absent)" >&2
    echo "    present: ${staged#"${PROJ_DIR}/"}" >&2
    echo "    cause: PackageMaterializer flattens a transitive package's lib/ into the connector" >&2
    echo "           package's own lib/, but never its native/." >&2
    BRIDGED=1
  elif ! cmp -s "${PROBE_DIR}/${lib}" "${SDK_ASSETS}/${lib}"; then
    echo "  UPSTREAM GAP (pz finding 20, ON THE PROBE PATH): ${lib} is where the ALC looks, but it" >&2
    echo "    is not this host's build -- the file dlopen would take is the wrong architecture." >&2
    echo "    on the probe path: $(stat -Lc%s "${PROBE_DIR}/${lib}") bytes" >&2
    echo "    SDK-resolved ${RID}: $(stat -Lc%s "${SDK_ASSETS}/${lib}") bytes" >&2
    BRIDGED=1
  fi
done

if [[ "${BRIDGED}" == "1" ]]; then
  if [[ "${PZ_VERIFY_STRICT:-0}" == "1" ]]; then
    echo "FAIL: PZ_VERIFY_STRICT=1 and pz's materialized package is wrong (see the gaps above)." >&2
    exit 1
  fi
  # Deliberate, loud scaffolding for pz defects, NOT a fix. Everything below this line -- the ALC
  # load, the native P/Invoke through its unmanaged-DLL hook, the run, the retry -- is what those
  # gaps stop pz reaching on its own, and staging the SDK-resolved assets where pz should have put
  # them is the only way to test any of it. Only files pz already materialized WRONGLY are replaced,
  # and only the two native libraries are added, so a dependency pz fails to materialize at all
  # still surfaces as a failure below rather than being papered over. Delete this block, and run
  # with PZ_VERIFY_STRICT=1, the day pz materializes a connector package correctly.
  echo "  Bridging so the REST of the chain can still be tested. This is scaffolding: through pz"
  echo "  alone, this connector does not load. See docs/installing.md."
  for name in ${BAD_DLLS[@]+"${BAD_DLLS[@]}"}; do
    cp "${SDK_ASSETS}/${name}" "${LIB_DIR}/${name}"
  done
  mkdir -p "${PROBE_DIR}"
  cp "${SDK_ASSETS}/libdelta_rs_bridge.so" "${SDK_ASSETS}/libdelta_kernel_ffi.so" "${PROBE_DIR}/"
fi

# The one assertion a broken state cannot satisfy: whatever sits on the probe path now -- put there
# by pz or staged by the bridge above -- must be byte-identical to the RID-correct library, or
# nothing below this line means anything.
for lib in libdelta_rs_bridge.so libdelta_kernel_ffi.so; do
  [[ -f "${PROBE_DIR}/${lib}" ]] || { echo "FAIL: ${lib} still absent from the probe path" >&2; exit 1; }
  cmp -s "${PROBE_DIR}/${lib}" "${SDK_ASSETS}/${lib}" || {
    echo "FAIL: ${lib} is on the probe path but is not the ${RID} build; the ALC would dlopen the" >&2
    echo "      wrong architecture. Nothing after this point would be a valid result." >&2
    exit 1; }
  echo "  native/${lib} ($(stat -Lc%s "${PROBE_DIR}/${lib}") bytes, matches the ${RID} build)"
done

# The lake is a store, not a DAG edge: nothing connects the pipeline that writes it to the pipeline
# that reads it back, so the two are independent flows and their order is the caller's to choose.
echo "-- pz run orders_delta (seed CSV -> Delta table, strategy: merge) --"
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
# writer renders a timestamp in the RUNNER's local zone (measured -- 23:59:00 UTC comes out
# "2026-01-04 01:59:00+02" under TZ=Europe/*), so pinning one string would pass in CI and fail on a
# developer's machine. What has to be true regardless of the zone is that three rows came back
# carrying a time of day: a write that was refused produces no report at all, a column that was
# dropped produces empty cells, and a value truncated to a date loses the HH:MM:SS.
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

# Native libraries are never unloaded once loaded into a process, and pz gives each connector package
# a collectible AssemblyLoadContext. `pz retry` is the command a user reaches for right after a
# failure, and it loads the connector again -- so a delta-rs-carrying ALC that could not be reloaded
# would fail exactly there. The failure is injected, not simulated: a regular file where the report
# sink needs a directory makes the SinkWrite fail with the Delta SourceLoad already succeeded, which
# is the shape retry has to reuse.
echo "-- Injecting a SinkWrite failure downstream of the Delta read --"
rm -rf "${PROJ_DIR}/out/report"
: > "${PROJ_DIR}/out/report"
if (cd "${PROJ_DIR}" && "${PZ}" run orders_report); then
  echo "FAIL: the injected failure did not fail the run" >&2; exit 1
fi

echo "-- pz retry: the connector ALC loads again, carrying both Rust libraries --"
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

# `pz retry` is a SEPARATE PROCESS, so everything above proves a fresh-process load and nothing about
# what happens when one process loads this connector's ALC more than once. That case is real: every
# `pz mcp` tool handler builds its own ConnectorHost and disposes it on the way out
# (`await using var connectorHost = host;`), so a long-lived MCP session loads and unloads the
# connector once per call. Native libraries are never unloaded from a process once loaded, so a
# second load resolving them again is exactly the thing nothing had tested.
#
# pz_entity_schema is the cheapest handler that reaches delta-rs: it opens the Delta table and
# returns "source":"fetched", i.e. GetSchemaAsync really ran through both Rust libraries. Three
# calls in one server process.
#
# Best-effort by design: it needs a JSON-RPC client over stdio, which bash cannot do honestly, so it
# is skipped with a note where python3 is absent rather than failing the run. Where python3 IS
# present, a crash or a wrong answer FAILS -- that would be a finding about pz's connector host.
echo "-- Loading the connector ALC three times in ONE process (pz mcp) --"
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
                 "clientInfo": {"name": "alc-reload-probe", "version": "1"}}})
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
    print("  FAIL: the pz mcp process died while reloading the connector ALC")
    print("  stderr:", "".join(errs)[-2000:])
    raise SystemExit(1)
if fetched != {2, 3, 4}:
    print(f"  FAIL: only calls {sorted(fetched)} opened the Delta table; expected 2, 3 and 4")
    print("  stderr:", "".join(errs)[-2000:])
    raise SystemExit(1)
print("  three ConnectorHosts built and disposed in one process; all three read the Delta schema")
print("  through both Rust libraries. Note this shows a repeated fresh-ALC load does not crash --")
print("  ConnectorHost.DisposeAsync requests Unload() but collection is GC-nondeterministic, so it")
print("  is NOT evidence that the previous ALC was actually collected.")
PYPROBE
    echo "FAIL: the in-process ALC reload probe failed (see above)" >&2; exit 1; }
fi

echo
echo "-- What the dependency costs (cold cache, measured, not predicted) --"
# Anchored on the key name rather than "everything before the first colon": the CLI prefixes the
# line with "info : " on some verbosities, and the loose form would then yield a path that does not
# exist. The vendor nupkg is what `dotnet pack` and `dotnet publish` above already restored, so its
# absence means the lookup broke, not that the file is optional -- and this is THE number Step 5
# exists to publish, so it fails rather than quietly printing one line fewer.
GLOBAL_PACKAGES="$(dotnet nuget locals global-packages --list | sed -n 's/.*global-packages: *//p' | head -1)"
VENDOR_NUPKG="${GLOBAL_PACKAGES}/deltalake.net/${DL_VERSION}/deltalake.net.${DL_VERSION}.nupkg"
[[ -n "${GLOBAL_PACKAGES}" && -f "${VENDOR_NUPKG}" ]] || {
  echo "FAIL: could not locate the DeltaLake.Net ${DL_VERSION} nupkg to measure the download size." >&2
  echo "      looked for: ${VENDOR_NUPKG}" >&2
  echo "      (global-packages resolved to '${GLOBAL_PACKAGES}')" >&2
  exit 1; }
echo "  this connector's nupkg:             $(du -h "${PKG}" | cut -f1)"
echo "  DeltaLake.Net ${DL_VERSION} nupkg (all RIDs):  $(du -h "${VENDOR_NUPKG}" | cut -f1)  <- the download"
echo "  pz package cache after restore:     ${CACHE_AFTER_RESTORE}"
echo "  .pz/packages after restore:         ${PACKAGES_AFTER_RESTORE}"
echo "  .pz/packages after bridging:        $(du -shL "${PROJ_DIR}/.pz/packages" | cut -f1)"
echo "  the ${RID} native pair alone:  $(du -ch "${SDK_ASSETS}"/*.so | tail -1 | cut -f1)"
echo "  (a cache holding the RID-correct natives instead of the wrong ones would differ; today's"
echo "   cache number is what pz actually produced, wrong architecture included.)"

echo
if [[ "${BRIDGED}" == "1" ]]; then
  echo "PASS, WITH THE ASSET CHAIN BRIDGED: everything downstream of package materialization works"
  echo "end to end, and package materialization itself does not (pz findings 19, 20 and 21 above)."
  echo "Do not read this green line as 'the external-connector path works' -- it does not yet."
else
  echo "PASS: the external-connector path works end to end."
fi
