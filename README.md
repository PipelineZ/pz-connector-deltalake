# Pz.Connector.DeltaLake

Delta Lake source and sink connector for [PipelineZ](https://pipelinez.dev) (`pz`).

Reads via DuckDB's `delta` extension; writes (append/replace/merge) via delta-rs.

## Before you install it

**Against pz 0.2.2 this connector does not load from a restored package.** pz's package materializer
extracts the wrong target framework and the wrong RID out of a multi-targeted, multi-RID dependency,
and never places a dependency's native assets where the connector's load context probes. The causes
are entirely in pz and are written up, with what each one looks like when it bites, in
[`docs/installing.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/installing.md).

**It is a 222 MB download.** `DeltaLake.Net` ships every RID's Rust libraries in one package, and
`pz restore` prints nothing while fetching it. It is not hung — wait it out. 126 MB then lands in
`.pz/packages`, nearly all of it the two Rust libraries.

## Documentation

- [`docs/installing.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/installing.md) — the external-connector path, what it costs, and what
  currently blocks it
- [`docs/compatibility.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/compatibility.md) — what is proven, on which backend, against which
  version, and what is merely shipped
- [`docs/reference/write.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/reference/write.md) — `merge`, `schema_policy`, partition values,
  and what a merge costs
- [`docs/limitations.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/limitations.md) and [`docs/troubleshooting.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/troubleshooting.md)

`samples/delta-roundtrip/` is a runnable project that seeds a Delta table from a CSV and reads it
back; `scripts/verify-external-connector.sh` runs it end to end against a locally packed build.
