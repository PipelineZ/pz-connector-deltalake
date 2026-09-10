# Pz.Connector.DeltaLake

Delta Lake source and sink connector for [PipelineZ](https://pipelinez.dev) (`pz`).

Reads through DuckDB's `delta` extension, so rows never enter .NET. Writes — `append`, `replace`,
`merge` — through delta-rs.

## Read this before you install it

**1. It requires pz 0.5.1 or newer, and runs in its own process.** pz spawns the Native AOT
connector binary the package ships for your platform and talks to it over the connector process
protocol (PCP); nothing from this package is loaded into pz itself. The connector is written against
`Pz.Connectors.Abstractions` 0.6.1 and served by `Pz.Connectors.Sdk` 0.6.1. Verified on linux-x64
against the released pz 0.5.1: `scripts/verify-external-connector.sh` restores, runs both
directions, retries, and passes the PCP conformance vectors end to end.

**2. It is a 200 MB download.** The package ships a Native AOT connector binary for four
platforms, each beside its own copy of delta-rs's two Rust libraries, and `pz restore` fetches the
whole nupkg before materializing only your platform's files. `pz restore` prints nothing while
fetching it. It is not hung — wait it out. 150 MB then lands in `.pz/packages`: a 13 MB binary and
the 138 MB `linux-x64` Rust pair. All measured — the four-platform package is the 0.2.0 release as pushed, the rest on linux-x64
with a cold cache. `docs/installing.md` has the full table.

**3. Platforms.**

| RID | status |
|---|---|
| `linux-x64` | **the only platform anything here has been run on** |
| `linux-arm64`, `osx-arm64`, `win-x64` | shipped — a binary and the matching Rust pair are in the package — never tested here |
| `osx-x64` | **not shipped.** `DeltaLake.Net` has the Rust pair for it, but the package publishes the SDK's default platform set; ask if you need it |
| `linux-musl-x64` (Alpine) | pz resolves a musl host to the `linux-x64` binary through the RID graph, but that binary is linked against glibc — **never tested here** |
| `win-arm64` | **unsupported** — `DeltaLake.Net` ships no Rust libraries for it |

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

`root:` must be absolute. This connector declares no project-directory anchor, so a relative root
would resolve against the working directory of a connector process you never launched — it refuses
one (PZDL0101) rather than guess. Reading it from the environment keeps the project portable.

**`project.yml`** — the connector is declared by package id, exactly as any other:

```yaml
name: delta_roundtrip
version: 0.1.0

connectors:
  - package: Pz.Connector.DeltaLake
    version: 0.2.0
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
pz restore
pz run orders_delta      # data/orders.csv -> a Delta table under $DELTA_LAKE_ROOT
pz run orders_report     # that Delta table -> out/report/orders_report/*.csv
```

`Pz.Connector.DeltaLake` is on nuget.org, so a bare `pz restore` resolves it from there. Pointing
`--feeds` at a local folder is only needed when testing a package you packed yourself, before it is
released — `scripts/verify-external-connector.sh` does exactly that.

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

19–39×, widening with table size. The lever is partitioning the table (`partition_by:`) and putting
the partition column in `keys:`. See
[`docs/how-to/partitioned-tables.md`](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/how-to/partitioned-tables.md).

`merge_predicate:` is the other lever, and it is sharp: **a row it excludes is duplicated, not
skipped.** Read
[`docs/how-to/tune-a-slow-merge.md`](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/how-to/tune-a-slow-merge.md)
before reaching for it.

## Documentation

**Start here if Delta Lake is new to you.**
[`docs/delta-lake-primer.md`](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/delta-lake-primer.md)
— Delta is a protocol, not a file format, and nearly every surprising thing about this connector
traces to protocol behaviour. The transaction log, versions and time travel, why `remove` does not
delete, partitioning, statistics, protocol versions, and concurrency — with real log excerpts.

**Concepts**

- [`docs/concepts/architecture.md`](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/concepts/architecture.md)
  — two engines, one connection, and the Rust library in your process
- [`docs/concepts/delivery-guarantees.md`](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/concepts/delivery-guarantees.md)
  — how Delta maps onto pz's matrix, and exactly which crash window duplicates

**Reference**

- [`docs/reference/connection.md`](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/reference/connection.md)
  — `root:`, credentials, and how each engine gets them
- [`docs/reference/read.md`](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/reference/read.md)
  — `path`, `version`, `union_by_name`, and what is pushed down
- [`docs/reference/write.md`](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/reference/write.md)
  — strategies, `keys`, `merge_predicate`, `schema_policy`, partition values, and what a merge costs

**How to**

- [incremental ingest with a watermark](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/how-to/incremental-ingest.md)
- [upsert with `keys`](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/how-to/upsert-with-keys.md)
- [partitioned tables](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/how-to/partitioned-tables.md)
- [tuning a slow merge](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/how-to/tune-a-slow-merge.md)
- [S3 credentials and multi-writer safety](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/how-to/s3-credentials.md)
- [Azure and ADLS credentials](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/how-to/azure-credentials.md)
- [reading a table another engine writes](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/how-to/read-a-table-another-engine-writes.md)
- [time travel](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/how-to/time-travel.md)

**Operations**

- [`docs/installing.md`](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/installing.md)
  — the external-connector path, what it costs, and what currently blocks it
- [`docs/compatibility.md`](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/compatibility.md)
  — what is proven, on which backend, against which version, and what is merely shipped
- [`docs/troubleshooting.md`](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/troubleshooting.md)
  — every `PZDL` code, plus the symptoms that arrive with no code at all
- [`docs/limitations.md`](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/limitations.md)
  — what this connector cannot do, and the measured behaviours to plan around

## What is proven

Every backend claim in this repository is a test, and the gaps are stated rather than left to
assumption. In short: local filesystem and S3 (MinIO) are covered for `append`, `replace`, `merge`,
read-back through both engines, and concurrent commits; **Azure `replace`, `merge` and concurrency are
not proven**; and **no test here has ever talked to Amazon S3 or to Azure Storage** — both are
emulators. [`docs/compatibility.md`](https://github.com/PipelineZ/pz-connector-deltalake/blob/main/docs/compatibility.md)
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

`scripts/verify-external-connector.sh` publishes the connector for this machine's platform, packs
it, and runs `samples/delta-roundtrip/` end to end against the released pz: restore, both
directions, `pz retry`, the PCP conformance vectors, and repeated spawns from one `pz mcp` process.
A release publishes four platforms first (`.github/workflows/release.yml`):

```bash
for rid in linux-x64 linux-arm64 osx-arm64 win-x64; do
  dotnet restore src/Pz.Connector.DeltaLake -r "$rid"
  dotnet publish src/Pz.Connector.DeltaLake -c Release -r "$rid" --no-restore
done
dotnet pack src/Pz.Connector.DeltaLake -c Release -o packages
```

## Licence

MIT — see [`LICENSE`](LICENSE).
