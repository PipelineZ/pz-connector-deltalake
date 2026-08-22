# Reading a table another engine writes

Delta is a protocol, so a table is not "a Spark table" or "a delta-rs table" — it is a table, and any
engine that implements the protocol version it declares can read it. That is the theory. This page is
about the parts of it this repository can and cannot vouch for.

## Point the connection at it

Nothing special:

```yaml
warehouse:
  connector: deltalake
  root: s3://lakehouse/gold
  entities:
    dim_customer:
      read:
```

`root:` is where the place is, the entity is the table under it, and `path:` overrides the layout when
the table is not simply `<root>/<entity>`.

## What is proven, and what is not

| | status |
|---|---|
| DuckDB `delta_scan` reads what this connector writes | **proven** — every write test in this repository asserts the round trip through the other engine |
| delta-rs reads what this connector writes | proven |
| this connector reads a table Spark or Databricks wrote | **not tested.** No test here has ever seen a Spark-written table |
| deletion vectors, column mapping, change data feed | **not tested**, in either direction. This connector writes none of them, and whether a read succeeds is DuckDB's delta extension's business, not something measured here |

That table is the whole claim. Reads go through DuckDB's `delta` extension, so a table it supports
will be read correctly and a table it does not will fail at scan time with DuckDB's own error rather
than a `PZDL` code.

## Protocol versions

This connector's stack writes `minReaderVersion: 1, minWriterVersion: 2` — measured. Those are the
widest-compatibility values, so a table it creates should be readable by anything.

The constraint runs the other way: a table written with a **higher** protocol version, or with named
table features, needs a reader that understands them. An engine too old should refuse rather than
misread — that is what the version numbers are for — but "should" is the protocol's promise, not one
measured here.

## Files that disagree on column order

A table written by several engines over time can hold data files whose columns are in different
orders. `union_by_name: true` tells the scan to unify by name rather than by position:

```yaml
warehouse:
  connector: deltalake
  root: s3://lakehouse/gold
  entities:
    dim_customer:
      read:
        union_by_name: true
```

It is emitted only because it was measured to be accepted by `delta_scan` on DuckDB 1.5.5 — the
version pz pins today. This connector references no DuckDB of its own, so a pz release that moves
DuckDB moves this with it.

## Writing into a table someone else created

This works, with three things to know.

- **Partitioning is the table's, not yours.** `partition_by` is honoured only when a table is created.
  Declaring a set that differs from the table's is refused with **PZDL0301**; declaring nothing at all
  against an already-partitioned table is fine, and the partition-value guards still apply.
- **`schema_policy` decides what a schema difference means.** The default `fail_on_change` refuses a
  column the table lacks (PZDL0301); `evolve` adds it, and the rows already in the table read null in
  it. A column that `append` or `merge` adds must be **nullable** — Delta has no value to give it in
  rows those strategies leave in place. `replace` is exempt, because it rewrites every file.
- **A merge cannot see duplicates that are already there.** Delta enforces no primary key. If the
  other engine left two rows for one key, a merge updates both and leaves both.

## Two ways to break a shared table

- **Do not delete data files by hand.** The log names them; it cannot notice one is gone, and a reader
  replaying the log will ask for a file that is not there.
- **Be careful with vacuum retention.** Deleting files a reader has already resolved from the log —
  or that an older version still needs — breaks that reader or that version. This connector never
  vacuums, so this is about the *other* engine's maintenance jobs running while pz reads.
