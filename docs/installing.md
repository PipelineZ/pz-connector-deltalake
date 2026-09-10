# Installing this connector into a pz project

This page is about the path a stranger takes: declare the connector by package id in `project.yml`,
run `pz restore`, run `pz run`. `samples/delta-roundtrip/` is that project, and
`scripts/verify-external-connector.sh` is that path exercised end to end.

**This connector requires pz 0.5.1 or newer, and runs in its own process.** Every third-party
connector does (PZ0360): the package ships a Native AOT binary per platform, its manifest says
`runtime: "process"`, and pz spawns that binary and talks to it over the connector process protocol
(PCP). Nothing from this package is loaded into pz. The connector compiles against
`Pz.Connectors.Abstractions` 0.6.1 and is served by `Pz.Connectors.Sdk` 0.6.1, which also
generates the manifest by running the binary — so the manifest and the handshake cannot disagree.

Verified on linux-x64 against the released 0.5.1: `scripts/verify-external-connector.sh` publishes
and packs the connector, restores it from a local feed, asserts that pz materialized the binary and
both Rust libraries byte-for-byte under the package's `native/` directory, runs the merge and the
`delta_scan` read-back, proves `pz retry` reuses the staged extraction, and runs the PCP
conformance vectors (`pz connector test`) against the materialized package.

**Spawning the connector repeatedly from one pz process works.** Measured: three `pz mcp` tool
calls in one server, each building and disposing its own connector host — which now means one
connector process per call — each opening the Delta table through both Rust libraries.

**The conformance read vectors do not apply.** `pz connector test` drives its read probes on the
universal Arrow tier, and this source is native-scan only, so those vectors fail with PZ0312 by
design; the verify script therefore probes the write side only.

**A green end-to-end run is not blanket validation, and one gap is worth naming.** Through pz,
`ArrowInterop.NormalizeNativeArrowSchema` forces every field `nullable: true` before a batch reaches
a sink, so an end-to-end run exercises **none** of this connector's nullability rules — not the
`NOT NULL`-addition refusal, not its `replace` exemption, not the pre-existing-column mirror. Their
only coverage is this repository's own test suite. A green `pz run` means the path works; it does not
mean every guard on the path ran.

## `root:` must be absolute, so the sample reads it from the environment

A connector receives a project-directory anchor only by declaring one: a package whose manifest
carries `"projectDirectoryAnchor": true` (the SDK's `PzProjectDirectoryAnchor` property) receives
the project directory as a `base_dir` option, the way `localfiles` does. **This connector does not
declare it**: the sample's `${DELTA_LAKE_ROOT}` is explicit about where the lake lives, and an
absolute root is right for `s3://`-family roots regardless.

A relative `root:` would otherwise resolve against the working directory of a connector process the
user never launched, so this connector refuses one with `PZDL0101` rather than guess.

Give it an absolute local path or an `s3://`/`az://`-family URI. The sample uses an environment
variable so the project stays portable:

```yaml
lake:
  connector: deltalake
  root: ${DELTA_LAKE_ROOT}
```

## What the dependency costs, and how long it looks broken

Measured on linux-x64 with a cold cache, `DeltaLake.Net` 0.33.0, `Pz.Connectors.Sdk` 0.5.1:

| | |
|---|---|
| the released nupkg — four platforms, each a Native AOT binary plus its Rust pair | **200 MB**, and this is the download |
| the same package built for one platform (what the verify script packs) | **52 MB** |
| `.pz/packages` as pz materializes it — this platform's files only | **150 MB** |
| the connector binary alone | 13 MB |
| the `linux-x64` Rust pair alone | 138 MB |

Sizes are `du -h` (so MiB), measured: the four-platform line is the 0.2.0 package as pushed to
nuget.org, the rest on linux-x64 with a cold cache. The Rust pair is the floor: it is already
stripped, and a self-contained CoreCLR single file in place of the native image was 51 MB per
platform — 348 MB for four, over nuget.org's 250 MB package cap. Compared with the previous
in-process packaging's 222 MB download, what lands on disk shrank as well, because only your
platform's Rust pair is materialized.

**`pz restore` looks hung and is not.** A 200 MB download behind a progress-free command is a minute
or more on a normal connection, and the first thing a new user does after 90 seconds of silence is
kill it and conclude the connector is broken. Wait it out. The second restore is a cache hit and
prints in under a second.

The first `pz run` also downloads DuckDB's `delta` extension the first time it runs on a machine;
that one is small but it is a second network round trip, so a fully offline first run is not
possible.
