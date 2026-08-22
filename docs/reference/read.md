# Reading a Delta table

A read is a DuckDB `delta_scan`. **The rows never enter .NET**: DuckDB opens the table, reads the
transaction log, prunes files with the log's own statistics, and scans the Parquet directly into the
run's staging database. This connector's read half contributes the scan fragment, the extension
loads, and one scoped secret — nothing else.

```yaml
lake:
  connector: deltalake
  root: /srv/lake
  entities:
    orders:
      read:
```

```sql
select dt, count(*) from {{ source('lake', 'orders') }} group by dt
```

Every option below may be declared under `entities: <e>: read:` **or** as a `source()` keyword
argument — never both, and never merged. The names are identical on both surfaces, so moving one is
cut-and-paste.

## Options

| option | type | default | notes |
|---|---|---|---|
| `path` | string | the entity's own name | The table's location under `root:`. An absolute path, or one carrying its own scheme, ignores `root:`. Calendar tokens (`{yyyy}/{MM}/{dd}`) are refused: Delta partitions declaratively by column value and has no templated path to expand, so a token here would be taken literally and find nothing. |
| `version` | integer, ≥ 0 | the table's latest version | Time travel. Reads the table as of that commit — see [how-to/time-travel.md](../how-to/time-travel.md). A value that is not a non-negative integer fitting in 64 bits is refused with PZDL0202 before anything is opened; a version the transaction log does not HOLD is not detected until the table is opened, and surfaces as PZDL0201 from the schema probe. Declared on a write, it is PZDL0105. |
| `union_by_name` | boolean | false | Unifies columns across data files by NAME rather than by position, for a table whose files disagree on column order. Accepted by `delta_scan` on DuckDB 1.5.5, which is the version pz's own `Pz.DuckDb` pins today and the one this repository's tests measure against — this connector references no DuckDB of its own and cannot hold that version still. Emitted only because the acceptance was measured, not because the parameter is documented to exist. |

## What the connector pushes down, and what it does not

- **Projection and predicate pushdown are DuckDB's**, from its own parse of the pipeline SQL. This
  connector declares the capability and then stays out of the way.
- **An incremental window is pushed as a predicate.** When the dataset carries a watermark, the
  fragment is wrapped as `(select * from delta_scan(…) where "cursor" > 'value')` — with `>=` when
  the engine hands over an inclusive lower bound, and ` and "cursor" <= 'upper'` for a bounded
  window. That predicate is the whole point: it is what lets DuckDB skip data files using the
  per-file `minValues`/`maxValues` the transaction log carries. See
  [how-to/incremental-ingest.md](../how-to/incremental-ingest.md).
- **`pushdown_filters => …` is never emitted.** Probed against DuckDB 1.5.5 with the `delta`
  extension: the parameter name is recognized, but every value tried — `true`, `false`, `constant`,
  `dynamic`, `random`, the bare integer `1` — is rejected as `Unknown Filter pushdown mode`. There
  is no known-good spelling, so the fragment omits it rather than guess one that would fail at scan
  time against real data. Filter pushdown still happens; it just cannot be requested explicitly.

## Reads are native-scan only

The source implements `INativeOnlySource`. There is no universal-tier fallback to force, and asking
for one (`engine.force_universal`, `files_per_partition`) fails with **PZ0312** rather than
silently doing something slower. Routing reads through delta-rs would pull Arrow batches into .NET
only to hand them straight back to DuckDB.

One consequence worth knowing: the *first* `pz run` on a machine downloads DuckDB's `delta`
extension. It is small, but it is a network round trip, so a fully offline first run is not
possible.

## The schema comes from delta-rs, not from DuckDB

pz needs a dataset's declared schema to compile the DAG, before any scan runs, and DuckDB's `delta`
extension has no way to hand that back ahead of a scan. So the one read-side call into delta-rs is
the schema probe: it opens the table, reads `ITable.Schema()`, and closes it. **DuckDB owns the read
data plane; delta-rs owns writes and table metadata.**

Delta declares its own schema in the transaction log, so `SchemaInferred` is `false` — pz's
integer-inference lint would be a false positive against a table that already knows its types.

## No `columns:` contract is needed for an incremental read

`where cursor > {{ watermark(...) }}` in the pipeline's own SQL is the declaration, and the
watermark's type comes from the stored watermark. A `columns:` contract is only needed for the
bounded-window trio (`initial`/`max_window`/`until`), whose bounds must be computable before the
first extraction.
