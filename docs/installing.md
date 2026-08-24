# Installing this connector into a pz project

This page is about the path a stranger takes: declare the connector by package id in `project.yml`,
run `pz restore`, run `pz run`. `samples/delta-roundtrip/` is that project, and
`scripts/verify-external-connector.sh` is that path exercised end to end.

**This connector requires pz 0.3.0 or newer**, and compiles against `Pz.Connectors.Abstractions`
0.3.0. Verified on linux-x64 against the released 0.3.0: `scripts/verify-external-connector.sh`
reports **no gaps at all** and passes under `PZ_VERIFY_STRICT=1` — pz materializes
`lib/net9.0/DeltaLake.dll`, puts the `linux-x64` Rust libraries on the connector's probe path
byte-for-byte, and the merge, the `delta_scan` read-back and the `pz retry` reuse all run with
nothing staged around them. `.pz/packages` measures 277 MB, the RID-correct pair.

`scripts/verify-external-connector.sh` compares what pz materialized against what
`dotnet publish -r <rid>` resolves from the same packages, so it doubles as a regression check on
that materialization. Run it with `PZ_VERIFY_STRICT=1` to fail the build on any gap it finds.

**Loading the connector repeatedly within one process does not crash.** Measured: three `pz mcp`
tool calls in one server, each building and disposing its own `ConnectorHost`, each opening the
Delta table through both Rust libraries. `ConnectorHost` requests `Unload()` on dispose but
collection is GC-nondeterministic, so this is not evidence that a previous load context was actually
collected — only that repeated load/unload has not been observed to crash.

**A green end-to-end run is not blanket validation, and one gap is worth naming.** Through pz,
`ArrowInterop.NormalizeNativeArrowSchema` forces every field `nullable: true` before a batch reaches
a sink, so an end-to-end run exercises **none** of this connector's nullability rules — not the
`NOT NULL`-addition refusal, not its `replace` exemption, not the pre-existing-column mirror. Their
only coverage is this repository's own test suite. A green `pz run` means the path works; it does not
mean every guard on the path ran.

## `root:` must be absolute, so the sample reads it from the environment

pz hands a connector no project-directory anchor. The `base_dir` option that lets `localfiles`
resolve a relative `path:` is injected by the CLI for `localfiles` and `sqlite` by name; nothing
reaches a third-party connector by default. pz 0.3.0 also makes a project-directory anchor available
declaratively — a connector opts in with `"projectDirectoryAnchor": true` in its `pz.connector.json`
and receives `base_dir` the way `localfiles` does — but **this connector does not declare it**: the
sample's `${DELTA_LAKE_ROOT}` is explicit about where the lake lives, and an absolute root is right
for `s3://`-family roots regardless.

A relative `root:` would otherwise resolve against whatever directory pz was launched from, so this
connector refuses one with `PZDL0101` rather than guess.

Give it an absolute local path or an `s3://`/`az://`-family URI. The sample uses an environment
variable so the project stays portable:

```yaml
lake:
  connector: deltalake
  root: ${DELTA_LAKE_ROOT}
```

## What the dependency costs, and how long it looks broken

Measured on linux-x64 with a cold cache, `DeltaLake.Net` 0.33.0:

| | |
|---|---|
| this connector's own nupkg | 52 KB |
| `DeltaLake.Net` nupkg — every RID in one package | **222 MB**, and this is the download |
| `~/.pz/cache` after one restore | 125 MB |
| `.pz/packages` as pz materializes it | **277 MB** — the RID-correct pair, nothing bridged |
| the `linux-x64` native pair alone | 138 MB |

Sizes are `du -h` (so MiB), measured, none projected.

**`pz restore` looks hung and is not.** A 222 MB download behind a progress-free command is a minute
or more on a normal connection, and the first thing a new user does after 90 seconds of silence is
kill it and conclude the connector is broken. Wait it out. The second restore is a cache hit and
prints in under a second.

The first `pz run` also downloads DuckDB's `delta` extension the first time it runs on a machine;
that one is small but it is a second network round trip, so a fully offline first run is not
possible.
