# Pz.Connector.DeltaLake

Delta Lake source and sink connector for [PipelineZ](https://pipelinez.dev) (`pz`).

Reads through DuckDB's `delta` extension, so rows never enter .NET. Writes — `append`, `replace`,
`merge` — through delta-rs.

## Read this before you install it

**1. Against pz 0.2.2 this connector does not load from a restored package.** pz's package
materializer extracts the wrong target framework and the wrong RID out of a multi-targeted, multi-RID
dependency, and never places a dependency's native assets where the connector's load context probes.
The causes are entirely in pz. They are written up, with what each one looks like when it bites, in
[`docs/installing.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/installing.md),
and `scripts/verify-external-connector.sh` detects them and stages around them so the rest of the
chain can still be exercised.

> **All of it is fixed in pz 0.3, which is not released yet.**
> [coccor/pz#16](https://github.com/coccor/pz/pull/16) closes these and the `partition_by` gap below.
> Verified on linux-x64 against a pz built from that PR: the verify script reports no gaps and passes
> under `PZ_VERIFY_STRICT=1`, and a `partition_by: ['dt']` Delta table comes out genuinely
> partitioned. Until pz 0.3 is on nuget.org, points 1–3 are what you get.

**2. It is a 222 MB download.** `DeltaLake.Net` ships every RID's Rust libraries in one package, and
`pz restore` prints nothing while fetching it. It is not hung — wait it out. 125 MB then lands in
`~/.pz/cache` and 126 MB in `.pz/packages` — and that 126 MB is **the wrong RID's** Rust libraries,
per point 1, which is why it is smaller than the 138 MB the `linux-x64` pair alone measures. Staging
the correct pair alongside takes the directory to 263 MB. All measured on linux-x64 with a cold
cache, none projected; `docs/installing.md` has the full table.

**3. Platforms.**

| RID | status |
|---|---|
| `linux-x64` | **the only platform anything here has been run on** |
| `linux-arm64`, `osx-x64`, `osx-arm64`, `win-x64` | shipped by `DeltaLake.Net`, never tested here |
| `linux-musl-x64` (Alpine) | **unsupported on pz 0.2.2** — it matches native assets by exact RID with no RID-graph fallback, so a musl host never matches `linux-x64`; pz 0.3 selects through the RID graph, making it reachable but still untested |
| `win-arm64` | **unsupported** — `DeltaLake.Net` ships no assets for it |

Shipped is not tested. CI runs ubuntu only, and deliberately: the object-store suites cannot pull
Linux images on a Windows runner.

## A working example

`samples/delta-roundtrip/` is this, runnable.

**`connections.yml`** — a connection is a place with credentials, an entity is a table in it, and the
direction is the function you call:

```yaml
seed:
  connector: localfiles
  entities:
    orders:
      read:
        path: data/orders.csv
        format: csv
        columns:
          id: bigint
          dt: varchar
          placed_at: timestamp
          amount: double

lake:
  connector: deltalake
  root: ${DELTA_LAKE_ROOT}

report:
  connector: localfiles
  root: out/report
```

`root:` must be absolute. pz hands a third-party connector no project-directory anchor, so a relative
root would resolve against whatever directory pz was launched from — this connector refuses one
(PZDL0101) rather than guess. Reading it from the environment keeps the project portable.

**`project.yml`** — the connector is declared by package id, exactly as any other:

```yaml
name: delta_roundtrip
version: 0.1.0

connectors:
  - package: Pz.Connector.DeltaLake
    version: 0.1.0
```

**`pipelines/orders_delta.sql`** — an upsert into a table the first run creates:

```sql
INSERT INTO {{ sink('lake', 'orders', strategy: 'merge', keys: ['id']) }}
select id, dt, placed_at, amount
from {{ source('seed', 'orders') }}
```

**`pipelines/orders_report.sql`** — reading it back, without the rows leaving DuckDB:

```sql
INSERT INTO {{ sink('report', 'orders_report', strategy: 'replace', format: 'csv') }}
select dt, count(*) as orders, sum(amount) as total, max(placed_at) as last_placed
from {{ source('lake', 'orders') }}
group by dt
order by dt
```

**Run it.** Name the flows, in order:

```bash
export DELTA_LAKE_ROOT="$PWD/out/lake"
pz restore --feeds <your-local-folder-feed> --feeds https://api.nuget.org/v3/index.json
pz run orders_delta      # data/orders.csv -> a Delta table under $DELTA_LAKE_ROOT
pz run orders_report     # that Delta table -> out/report/orders_report/*.csv
```

`--feeds` is there because `0.1.0` is this connector's first tagged release: until that tag exists
there is nothing on nuget.org under this id, so point the first feed at a folder holding a package
you packed yourself and pin that version. `scripts/verify-external-connector.sh` does all of it for
you. Once the release is out, a bare `pz restore` is enough.

Two runs, not one: the lake is a store, not a DAG edge. pz derives edges from `ref()`, `source()` and
`sink()` calls, and nothing connects the pipeline that writes `lake.orders` to the one that reads it,
so bare `pz run` refuses with PZ0215 and `pz run --all` would schedule the read before the write.

Re-running `orders_delta` updates those rows in place rather than duplicating them.

## Before you merge into a large table

**Merge cost follows the files the merge touches, not the rows it changes.** Parquet files are
immutable, so updating one row rewrites the file it lives in. Measured (`DeltaLake.Net` 0.33.0, local
filesystem, 200 partitions, 1,000 source rows scattered across 5 of them):

| table rows | partition column not joined | partition column joined |
|---|---|---|
| 500 000 | 1 601 ms | 85 ms |
| 2 000 000 | 6 446 ms | 253 ms |
| 8 000 000 | 23 793 ms | 604 ms |

19–39×, widening with table size. The lever is partitioning the table and putting the partition
column in `keys:` — and **that is currently unreachable through pz**, which reads `partition_by:` as a
calendar-token path template and refuses it (PZ0219) for a store that partitions by column value. A
Delta table written through pz today is unpartitioned.

`merge_predicate:` is the other lever, and it is sharp: **a row it excludes is duplicated, not
skipped.** Read
[`docs/how-to/tune-a-slow-merge.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/how-to/tune-a-slow-merge.md)
before reaching for it.

## Documentation

**Start here if Delta Lake is new to you.**
[`docs/delta-lake-primer.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/delta-lake-primer.md)
— Delta is a protocol, not a file format, and nearly every surprising thing about this connector
traces to protocol behaviour. The transaction log, versions and time travel, why `remove` does not
delete, partitioning, statistics, protocol versions, and concurrency — with real log excerpts.

**Concepts**

- [`docs/concepts/architecture.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/concepts/architecture.md)
  — two engines, one connection, and the Rust library in your process
- [`docs/concepts/delivery-guarantees.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/concepts/delivery-guarantees.md)
  — how Delta maps onto pz's matrix, and exactly which crash window duplicates

**Reference**

- [`docs/reference/connection.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/reference/connection.md)
  — `root:`, credentials, and how each engine gets them
- [`docs/reference/read.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/reference/read.md)
  — `path`, `version`, `union_by_name`, and what is pushed down
- [`docs/reference/write.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/reference/write.md)
  — strategies, `keys`, `merge_predicate`, `schema_policy`, partition values, and what a merge costs

**How to**

- [incremental ingest with a watermark](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/how-to/incremental-ingest.md)
- [upsert with `keys`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/how-to/upsert-with-keys.md)
- [partitioned tables](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/how-to/partitioned-tables.md)
- [tuning a slow merge](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/how-to/tune-a-slow-merge.md)
- [S3 credentials and multi-writer safety](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/how-to/s3-credentials.md)
- [Azure and ADLS credentials](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/how-to/azure-credentials.md)
- [reading a table another engine writes](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/how-to/read-a-table-another-engine-writes.md)
- [time travel](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/how-to/time-travel.md)

**Operations**

- [`docs/installing.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/installing.md)
  — the external-connector path, what it costs, and what currently blocks it
- [`docs/compatibility.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/compatibility.md)
  — what is proven, on which backend, against which version, and what is merely shipped
- [`docs/troubleshooting.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/troubleshooting.md)
  — every `PZDL` code, plus the symptoms that arrive with no code at all
- [`docs/limitations.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/limitations.md)
  — what this connector cannot do, and the measured behaviours to plan around

## What is proven

Every backend claim in this repository is a test, and the gaps are stated rather than left to
assumption. In short: local filesystem and S3 (MinIO) are covered for `append`, `replace`, `merge`,
read-back through both engines, and concurrent commits; **Azure `replace`, `merge` and concurrency are
not proven**; and **no test here has ever talked to Amazon S3 or to Azure Storage** — both are
emulators. [`docs/compatibility.md`](https://github.com/coccor/pz-connector-deltalake/blob/main/docs/compatibility.md)
is the matrix.

One gap is worth repeating because it is easy to miss: through pz,
`ArrowInterop.NormalizeNativeArrowSchema` forces every field `nullable: true` before a batch reaches a
sink, so a green end-to-end `pz run` exercises **none** of this connector's nullability rules. Their
only coverage is this repository's own suite.

## Building it

Requires the .NET 10 SDK. Docker is optional — the object-store suites skip cleanly without it.

```bash
dotnet build Pz.Connector.DeltaLake.slnx -c Release    # zero warnings required
dotnet test  Pz.Connector.DeltaLake.slnx -c Release --no-build
PZ_TESTS_OFFLINE=1 dotnet test Pz.Connector.DeltaLake.slnx -c Release --no-build
```

`scripts/verify-external-connector.sh` packs the connector and runs `samples/delta-roundtrip/` end to
end against it. `PZ_VERIFY_STRICT=1` makes it fail on pz's materializer defects instead of staging
around them — that is the mode that goes green the day pz fixes them.

## Licence

MIT (`PackageLicenseExpression` on the package; there is no `LICENSE` file in the repository yet).
