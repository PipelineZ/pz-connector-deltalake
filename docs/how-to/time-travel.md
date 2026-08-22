# Reading an earlier version of a table

Every Delta commit is a version. Reading version *N* replays commits `0 … N` and stops.

```yaml
lake:
  connector: deltalake
  root: /srv/lake
  entities:
    orders:
      read:
        version: 7
```

or at the call site:

```sql
select * from {{ source('lake', 'orders', version: 7) }}
```

Never both — an option belongs to one surface or the other, never merged (PZ0341).

`version` drives **both** the scan and the schema pz compiles the DAG against, so a pinned read of a
table whose schema has since widened compiles against the schema that version had.

## Rules

- **Non-negative integers only.** A value that is not a non-negative integer fitting in 64 bits is
  refused with **PZDL0202** before anything is opened.
- **Reads only.** `version` on a write is **PZDL0105**. Delta has no "write to an old version" — a
  restore is a new commit that reinstates old files, and this connector does not offer one.
- **A version the log does not hold is not detected until the table is opened**, and surfaces as
  PZDL0201 from the schema probe.

## What can take a version away from you

Two mechanisms, and both are worth understanding before building anything on a pinned version.

**Vacuum.** A `remove` action does not delete a file; vacuum does, for files no live version
references and older than a retention window. A version whose files have been vacuumed cannot be
replayed any more. This connector exposes no vacuum, so it will never do this to you — but anything
else writing that table might.

**Log truncation.** The commit files themselves can be cleaned up once checkpoints exist beyond them.
Note that this writer does not appear to make checkpoints on its own: measured, a table taken to 13
commits with `DeltaLake.Net` 0.33.0 has 13 JSON commit files, no `.checkpoint.parquet`, and no
`_last_checkpoint`.

## Uses that are worth it

- **Reproducing a run.** Pin the version a run read, and it reads the same rows however much the table
  has moved since.
- **Diffing two versions.** Two datasets over the same table at different versions, joined in the
  pipeline SQL. That is not a change data feed — it is a snapshot diff, and it cannot see a row that
  was inserted and deleted between the two versions.
- **Reading a table another writer is actively rewriting**, without racing it.

## What this is not

It is not incremental ingest. A version pin is fixed in the config; it does not advance with each
run, and pz's watermark machinery does not touch it. For "rows since last time", see
[incremental-ingest.md](incremental-ingest.md).
