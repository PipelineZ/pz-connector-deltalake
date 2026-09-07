# Architecture: two engines, one connection

This connector reads with one engine and writes with another. That is not an implementation detail
you can ignore — it is the reason platform problems here look the way they do, and the first thing to
know when something fails in a way a .NET stack trace does not explain.

```
                    ┌──────────────────────────────────────────┐
   read  ──────────►│  DuckDB + `delta` extension              │──► pz staging DuckDB
                    │  delta_scan(): log replay, file skipping │    (bytes never enter .NET)
                    │  and Parquet scan, all inside DuckDB     │
                    └──────────────────────────────────────────┘

                    ┌──────────────────────────────────────────┐
   write ──Arrow───►│  delta-rs, through DeltaLake.Net         │──► Delta table
                    │  append / replace / merge, commit        │
                    └──────────────────────────────────────────┘
```

## Why reads go through DuckDB

pz has two data-plane tiers: a **native** tier where a connector hands DuckDB a SQL fragment and the
bytes never enter .NET, and a **universal** tier of Arrow `RecordBatch` streams. A Delta read belongs
on the native tier, and nothing else would be an improvement:

- DuckDB's `delta` extension already does log replay, file skipping from the log's statistics,
  projection and predicate pushdown.
- Routing reads through delta-rs instead would pull Arrow batches into .NET only to hand them
  straight back to DuckDB.

So the source implements `INativeOnlySource`. There is no universal fallback to force; asking for one
fails with **PZ0312** rather than quietly doing something slower. What the source contributes is a
`delta_scan(...)` fragment, the `install`/`load` statements for the extensions it needs, and one
credential secret scoped to the connection's own `root`.

**The one read-side call into delta-rs is the schema probe.** pz needs a dataset's declared schema to
compile the DAG, before any scan runs, and DuckDB's delta extension cannot hand that back ahead of a
scan. So `GetSchemaAsync` opens the table through delta-rs, reads its schema, and closes it. The
split is: **DuckDB owns the read data plane; delta-rs owns writes and table metadata.**

## Why writes go through delta-rs

Because DuckDB cannot write Delta at all. `COPY … TO … (FORMAT delta)` does not exist — the extension
is read-only, verified against DuckDB 1.5.5 with the `delta` extension. So `TryGetNativeCopy` returns false
unconditionally and every write is a universal-path write: pz hands the sink Arrow `RecordBatch`es
and delta-rs turns them into Parquet files and a commit.

Three things follow from that, and each one shows up in the reference pages:

- **Batches are cloned on the way in.** A batch handed to `WriteBatchAsync` is engine-owned and its
  buffers may be recycled the moment the call returns, but delta-rs's write entry points take a
  *collection* — so the session must retain batches past the call. It clones rather than retaining
  the instance.
- **An `append` flushes in bounded generations** (`target_file_bytes`, default 128 MiB), so memory
  tracks a file rather than the whole write.
- **`replace` and `merge` buffer everything.** Their semantics are defined over the entire input, so
  for those the buffer *is* the write. That is a real memory limit on a large output.

## A Rust library is loaded into the connector's process — not into pz

`DeltaLake.Net` is a .NET binding over delta-rs. Two native Rust libraries come with it —
`libdelta_rs_bridge` and `libdelta_kernel_ffi` — and both are loaded by the connector process.

**How they get there.** The connector is served out of process by `Pz.Connectors.Sdk`: `dotnet
publish -r <rid>` produces a Native AOT binary with the two Rust libraries beside it,
`dotnet pack` ships one such directory per platform under `runtimes/<rid>/native/`, and `pz restore`
flattens this host's into `<package>/native/`. `pz run` spawns `native/Pz.Connector.DeltaLake` and
talks to it over the connector process protocol (PCP); the binary's own host resolves the Rust
libraries from its directory. Nothing here is loaded into pz, so pz's own Arrow version, DuckDB
version and trimming settings are not this connector's concern — only the Arrow *wire* format is
shared, across the PCP data plane.

**What this costs you.** Each platform carries its own Rust pair beside a 13 MB native image: 200 MB
for the four-platform package, of which 150 MB is materialized on a `linux-x64` host. The
Rust pair is the floor, which is why the binary is Native AOT rather than a self-contained CoreCLR
single file — the latter is 51 MB per platform, and four of those plus the Rust pairs exceed
nuget.org's 250 MB package cap. Sizes, timings, and
the platforms that are supported and unsupported are in [../installing.md](../installing.md) and the
README.

**And what it means when it breaks.** A failure inside a Rust library does not always arrive as a .NET
exception. It can be a `DllNotFoundException` (the library is not beside the binary), a `dlopen`
architecture error (the wrong platform's library is), or an abort of the connector process with no
managed exception at all — which pz reports as a lost connector rather than dying with it.
[../troubleshooting.md](../troubleshooting.md) names the shapes this connector has actually seen.

## Every delta-rs call runs on a 32 MiB stack

delta-kernel-rs can exhaust a default .NET thread stack on Unix, and a stack overflow **kills the
process** rather than raising something pz could report. The connector therefore owns the stack size
instead of asking you to set an environment variable: every delta-rs call is dispatched onto a thread
named `pz-deltalake` with a 32 MiB stack (reserved address space, not committed memory), one thread
per operation.

If you see a `pz-deltalake` frame in a crash dump, that is this gate — not a bug in it.

## Two APIs this connector will not call

`ITable.FileUrisAsync()` and `FilesAsync()` corrupt the heap. On a table with 320 files,
`FileUrisAsync` threw `ArgumentOutOfRangeException: byteCount ('-738195480') must be a non-negative
value` from the binding's FFI string-array marshalling, and the process then aborted with
`free(): invalid size` — a negative length reaching `Encoding.GetString` across the boundary, then a
bad `free`. Reproduced on `DeltaLake.Net` 0.33.0, linux-x64, .NET 10.

Nothing in this connector calls them, and nothing depends on them: DuckDB answers every file-listing
question the connector actually has. It is also the clearest single reason this connector lives
outside pz's own repository and release cadence — a heap-corrupting dependency inside the pz CLI
would be indistinguishable, to a user, from a pz bug.

## Where the connector's own logic lives

Most of this connector is not plumbing between the two engines. It is the guards:

| concern | what it does |
|---|---|
| root classification | one `root:` decides both the DuckDB extension + secret and the delta-rs storage options |
| credential translation | the same connection config becomes a DuckDB `CREATE SECRET` and a delta-rs storage-option map, choosing the SAME mechanism on both sides |
| merge statement generation | builds the full `MERGE` statement delta-rs requires, quoting every identifier |
| merge key resolution | resolves a key named twice in one write to its last row, because Delta's own `MERGE` cannot |
| partition predicate derivation | narrows a merge's target scan — only when every partition column is also a merge key, because otherwise it could duplicate rows |
| schema reconciliation | decides what `schema_policy` permits before anything is committed |
| error translation | turns a delta-rs failure into a `PZDL`-coded pz error, classified transient or not |

The two that are easiest to underestimate are merge key resolution and partition predicate
derivation. Both exist because the straightforward version of them silently duplicates rows;
[../reference/write.md](../reference/write.md) has the worked examples.

## What is NOT here

- **No hand-written `_delta_log` parser.** Reading the log is DuckDB's job on the read side and
  delta-rs's on the write side. This connector has never parsed a commit file.
- **No retry loop.** Connectors never retry internally. A lost commit race is reported as transient
  (PZDL0401) and pz's own retry policy decides.
- **No compaction, vacuum, or checkpoint verb.** pz has no command that maps to table maintenance, so
  these are deliberately not surfaced. See [../limitations.md](../limitations.md).
- **No change data feed and no `on_delete`.** The `ApplyDeletes` capability is deliberately not
  declared, so `on_delete: delete|soft` fails with PZ0339 rather than half-working.
