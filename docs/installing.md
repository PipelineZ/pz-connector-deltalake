# Installing this connector into a pz project

This page is about the path a stranger takes: declare the connector by package id in `project.yml`,
run `pz restore`, run `pz run`. `samples/delta-roundtrip/` is that project, and
`scripts/verify-external-connector.sh` is that path exercised end to end.

**This connector requires pz 0.3.0 or newer**, and compiles against `Pz.Connectors.Abstractions`
0.3.0. The first section says why an older pz cannot install it — kept because a lock file an older
pz wrote still carries the consequences, and because the failure modes are worth recognizing.

## Why pz 0.2.2 cannot load a restored package

> [!NOTE]
> **Fixed in pz 0.3.0** ([coccor/pz#16](https://github.com/coccor/pz/pull/16)). Verified on linux-x64
> against the released 0.3.0: `scripts/verify-external-connector.sh` reports **no gaps at all** and
> passes under `PZ_VERIFY_STRICT=1` — pz materializes `lib/net9.0/DeltaLake.dll`, puts the
> `linux-x64` Rust libraries on the connector's probe path byte-for-byte, and the merge, the
> `delta_scan` read-back and the `pz retry` reuse all run with nothing staged around them.
> `.pz/packages` measures 277 MB, the RID-correct pair rather than the wrong one.
>
> A `pz.lock.json` written by pz 0.2.2 records no archive paths and cannot be upgraded in place: pz
> 0.3.0 reports it as `PZ0321` and `pz restore` regenerates it.

`pz.lock.json` records each asset as a bare FILE NAME. `PackageMaterializer` then re-finds that name
in the .nupkg under a prefix alone — `lib/` for managed assemblies, `runtimes/` for native ones — and
extracts whichever archive entry matches first. The target framework and the RID that pz's resolver
had already selected are not carried across, so on any package that multi-targets or ships several
RIDs the wrong file wins, silently. The discarding starts one step earlier than the materializer:
`NuGetResolver` computes the nearest target framework and then keeps only the file names it maps to,
so the information is gone before anything is written to the lock — for the connector's own package
as much as for its dependencies. Three consequences, all measured on linux-x64 against
`DeltaLake.Net` 0.33.0:

| what pz materializes | what a `net10.0`/`linux-x64` host needs |
|---|---|
| `lib/net472/DeltaLake.dll` (181 248 bytes) | `lib/net9.0/DeltaLake.dll` (177 664 bytes) |
| `runtimes/linux-arm64/native/libdelta_rs_bridge.so` (85 470 856 bytes) | `runtimes/linux-x64/...` (98 576 408 bytes) |
| nothing at `Pz.Connector.DeltaLake/<version>/native/` | both `.so` files there |

The first surfaces as `MissingMethodException: Void DeltaLake.Table.TableStorageOptions.set_TableLocation(System.String)`
on the first write — the .NET Framework build of the vendor assembly has a different API surface than
the .NET 9 one. The second and third surface as a `DllNotFoundException`, or would surface as a
`dlopen` architecture error if the file were on the probe path at all:
`ConnectorLoadContext.LoadUnmanagedDll` looks in `<package>/native/` and nowhere else, and a
dependency package's own `native/` directory is never on that path — pz flattens a transitive
package's `lib/` into the connector package, but not its `native/`.

A fourth consequence has not bitten this connector but is the same defect: that `lib/` flattening
copies by file name with `overwrite: true`, so two dependencies shipping an assembly of the same name
silently overwrite each other in the connector's `lib/`. All four are one bug — the lock file stores
file names where it needs archive paths.

`scripts/verify-external-connector.sh` detects all three by comparing what pz materialized against
what `dotnet publish -r <rid>` resolves from the same packages, prints each one, and then stages the
correct assets so the rest of the chain can still be tested. Run it with `PZ_VERIFY_STRICT=1` to make
it fail on them instead — that is the mode that goes green the day pz fixes this.

**Everything downstream of that works.** With the correct assets in place, on linux-x64 against pz
0.2.2: the connector ALC loads it, both Rust libraries resolve through the unmanaged-DLL hook, a
merge writes a Delta table, a `delta_scan` reads it back, and `pz retry` reloads the whole thing in a
fresh process and reuses the staged Delta extraction instead of re-reading the table. Loading the
connector repeatedly **within one process** does not crash either: three `pz mcp` tool calls in one
server, each building and disposing its own `ConnectorHost`, each opening the Delta table through
both Rust libraries. That is what was observed, and it is the whole of it — `ConnectorHost` requests
`Unload()` on dispose but collection is GC-nondeterministic, so it is not evidence that the previous
load context was actually collected.

**But a green run there is not blanket validation, and one gap is worth naming.** Through pz,
`ArrowInterop.NormalizeNativeArrowSchema` forces every field `nullable: true` before a batch reaches
a sink, so an end-to-end run exercises **none** of this connector's nullability rules — not the
`NOT NULL`-addition refusal, not its `replace` exemption, not the pre-existing-column mirror. Their
only coverage is this repository's own test suite. A green `pz run` means the path works; it does not
mean every guard on the path ran.

## `root:` must be absolute, so the sample reads it from the environment

pz hands a connector no project-directory anchor. The `base_dir` option that lets `localfiles`
resolve a relative `path:` is injected by the CLI for `localfiles` and `sqlite` by name; nothing
reaches a third-party connector. A relative `root:` would therefore resolve against whatever
directory pz was launched from, so this connector refuses one with `PZDL0101` rather than guess.

Give it an absolute local path or an `s3://`/`az://`-family URI. The sample uses an environment
variable so the project stays portable:

```yaml
lake:
  connector: deltalake
  root: ${DELTA_LAKE_ROOT}
```

## `partition_by:` cannot be declared through pz 0.2.2

pz 0.2.2 reads `partition_by:` as ONE column whose value substitutes calendar tokens
(`{yyyy}/{MM}/{dd}`) in the sink's `path:`, and rejects the option with `PZ0219` whenever the path
carries no such tokens. Delta partitions declaratively by column value and has no templated path to
route into, so the two meanings never meet: **a Delta table written through pz 0.2.2 is
unpartitioned.** The connector's partitioning and its partition-pruned merge are reachable only when
it is driven directly.

What that costs is measured in `reference/write.md` — joining on the partition column is worth
19–39x on a merge.

> [!NOTE]
> **Fixed in pz 0.3.0.** `partition_by:` names the columns an output is partitioned by — a name or a
> list — and `path:` decides who lays them out: calendar tokens mean pz renders the layout
> (`PathTemplating`), no tokens mean the destination records its own
> (`ConnectorCapabilities.ColumnPartitionedWrites`, which this connector declares instead of
> `PathTemplating`, a flag it only ever held to get past the old gate).
>
> Verified end to end against the released pz 0.3.0. `samples/delta-roundtrip` declares
> `partition_by: ['dt']` with no `path:`, and the table pz writes carries
> `"partitionColumns":["dt"]` in `_delta_log/00000000000000000000.json` with `dt=2026-01-01`,
> `dt=2026-01-02` and `dt=2026-01-03` directories on disk. The partition-pruned merge is reachable
> through pz.

## `projectDirectoryAnchor`: a relative `root:` is possible from pz 0.3.0

The section below describes pz 0.2.2, where the project-directory anchor was injected by connector
NAME and so could never reach a third-party connector. pz 0.3.0 makes it declarative: a connector
opts in with `"projectDirectoryAnchor": true` in its `pz.connector.json` and receives `base_dir` the
way `localfiles` does. **This connector does not declare it** — the sample's `${DELTA_LAKE_ROOT}` is
explicit about where the lake lives, and an absolute root is right for `s3://`-family roots
regardless. Declaring it is a live option, not a limitation.

## What the dependency costs, and how long it looks broken

Measured on linux-x64 with a cold cache, `DeltaLake.Net` 0.33.0:

| | |
|---|---|
| this connector's own nupkg | 52 KB |
| `DeltaLake.Net` nupkg — every RID in one package | **222 MB**, and this is the download |
| `~/.pz/cache` after one restore | 125 MB |
| `.pz/packages` as pz 0.3.0 materializes it | **277 MB** — the RID-correct pair, nothing bridged |
| `.pz/packages` as pz 0.2.2 materialized it | 126 MB — and unusable, per the section above |
| `.pz/packages` after the verify script bridged 0.2.2 | 263 MB — the wrong-architecture pair still there and the right one beside it |
| the `linux-x64` native pair alone | 138 MB |

Sizes are `du -h` (so MiB), measured, none projected. The 263 MB figure is the bridged state, not
what a fixed pz would produce: it double-counts the Rust libraries. The design predicted ~143 MB
materialized on linux-x64 and the `linux-x64` native pair alone measures 143 718 640 bytes, so that
prediction held.

**`pz restore` looks hung and is not.** A 222 MB download behind a progress-free command is a minute
or more on a normal connection, and the first thing a new user does after 90 seconds of silence is
kill it and conclude the connector is broken. Wait it out. The second restore is a cache hit and
prints in under a second.

The first `pz run` also downloads DuckDB's `delta` extension the first time it runs on a machine;
that one is small but it is a second network round trip, so a fully offline first run is not
possible.
