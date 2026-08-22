# Installing this connector into a pz project

This page is about the path a stranger takes: declare the connector by package id in `project.yml`,
run `pz restore`, run `pz run`. `samples/delta-roundtrip/` is that project, and
`scripts/verify-external-connector.sh` is that path exercised end to end.

Read the first section before anything else. **Against pz 0.2.2 this connector does not load from a
restored package**, for reasons that are entirely in pz's package materializer and have nothing to do
with Delta.

## Against pz 0.2.2, a restored package cannot load

`pz.lock.json` records each asset as a bare FILE NAME. `PackageMaterializer` then re-finds that name
in the .nupkg under a prefix alone — `lib/` for managed assemblies, `runtimes/` for native ones — and
extracts whichever archive entry matches first. The target framework and the RID that pz's resolver
had already selected are not carried across, so on any package that multi-targets or ships several
RIDs the wrong file wins, silently. Three consequences, all measured on linux-x64 against
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

`scripts/verify-external-connector.sh` detects all three by comparing what pz materialized against
what `dotnet publish -r <rid>` resolves from the same packages, prints each one, and then stages the
correct assets so the rest of the chain can still be tested. Run it with `PZ_VERIFY_STRICT=1` to make
it fail on them instead — that is the mode that goes green the day pz fixes this.

**Everything downstream of that works.** With the correct assets in place, on linux-x64 against pz
0.2.2: the connector ALC loads it, both Rust libraries resolve through the unmanaged-DLL hook, a
merge writes a Delta table, a `delta_scan` reads it back, and `pz retry` reloads the whole thing in a
fresh process and reuses the staged Delta extraction instead of re-reading the table.

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

## `partition_by:` cannot be declared through pz

pz reads `partition_by:` as ONE column whose value substitutes calendar tokens (`{yyyy}/{MM}/{dd}`)
in the sink's `path:`, and rejects the option with `PZ0219` whenever the path carries no such tokens.
Delta partitions declaratively by column value and has no templated path to route into, so the two
meanings never meet: **a Delta table written through pz today is unpartitioned.** The connector's
partitioning and its partition-pruned merge are reachable only when it is driven directly.

What that costs is measured in `reference/write.md` — joining on the partition column is worth
19–38x on a merge, and that is the number currently out of reach through pz.

## What the dependency costs, and how long it looks broken

Measured on linux-x64 with a cold cache, `DeltaLake.Net` 0.33.0:

| | |
|---|---|
| this connector's own nupkg | 52 KB |
| `DeltaLake.Net` nupkg — every RID in one package | **222 MB**, and this is the download |
| `~/.pz/cache` after one restore | 125 MB |
| `.pz/packages` as pz materializes it today | 126 MB — and unusable, per the section above |
| `.pz/packages` with the RID-correct natives instead | ~140 MB (the `linux-x64` native pair is 138 MB of it) |

Sizes are `du -h`. The design predicted ~143 MB materialized on linux-x64; the `linux-x64` native
pair alone measures 143 718 640 bytes, so that prediction held.

**`pz restore` looks hung and is not.** A 222 MB download behind a progress-free command is a minute
or more on a normal connection, and the first thing a new user does after 90 seconds of silence is
kill it and conclude the connector is broken. Wait it out. The second restore is a cache hit and
prints in under a second.

The first `pz run` also downloads DuckDB's `delta` extension the first time it runs on a machine;
that one is small but it is a second network round trip, so a fully offline first run is not
possible.
