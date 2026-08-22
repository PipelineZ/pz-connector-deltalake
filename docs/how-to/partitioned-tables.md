# Partitioned Delta tables

**Read this first: `partition_by:` cannot be declared through pz today.** pz reads `partition_by:` as
ONE column whose value substitutes calendar tokens (`{yyyy}/{MM}/{dd}`) in the sink's `path:`, and
refuses the option with **PZ0219** when the path carries no such tokens. Delta partitions
declaratively by column value, with no templated path to route into, so the two meanings never meet:
**a Delta table written through pz is unpartitioned.**

Everything below is reachable when the connector is driven directly, through the ABI. It is documented
because the option exists, because it is the single biggest lever on merge cost, and because a table
another engine partitioned still reads and writes correctly here — only the *declaration* is out of
reach.

## What partitioning does

A partitioned table stores each partition's files under a directory named for its value, and records
the values in the transaction log:

```
orders/
  _delta_log/
  dt=2026-01-01/part-00000-….snappy.parquet
  dt=2026-01-02/part-00000-….snappy.parquet
```

Partition columns are **not stored in the data files** — measured; the Parquet file above holds `id`
and `amt` and no `dt`. An engine filtering on `dt` therefore prunes without opening a single file: it
reads the values out of the log. That is why the pruning is free, and why joining on the partition
column is worth 19–39× on a merge.

[../delta-lake-primer.md](../delta-lake-primer.md) has the log excerpts.

## Declaring it (driven directly)

```
partition_by: [dt]
```

as a write option, alongside `strategy` and `keys`. Two rules:

- **It is honoured only when the table is CREATED.** A run against a table an earlier run partitioned
  need not repeat it. A run that declares a *different* set than the table has is refused with
  **PZDL0301** — the guard's authority is the table's own partition columns, not this option.
- **It must be a list.** A bare `partition_by: dt` is **PZDL0108**, not silently read as "no
  partitioning": a quietly unpartitioned table is a permanent layout mistake.

## Choosing partition columns

- **Low cardinality.** Each distinct value is a directory and at least one file. A partition per
  customer id gives you a directory per customer.
- **Something your reads filter on.** Pruning only pays when queries name the column.
- **Something your merges join on.** The partition predicate that makes a merge fast is only derived
  when *every* partition column is also a merge key — see [tune-a-slow-merge.md](tune-a-slow-merge.md).

## Values a partition column cannot carry

**PZDL0406** covers two causes, on `append`, `replace` and `merge` alike — and they are caught at
**different times**, which decides what state your table is in afterwards.

**An empty value is refused before the batch is buffered.** Nothing is written. An empty partition
value does not survive the round trip: on a nullable partition column the write is accepted and the
rows read back with **null** — a silent change to your data — so this one is checked per batch, up
front, rather than translated from a failure that never comes. Both string and binary columns are
affected. A genuine null is not refused: a null written is a null read back.

Measured, and not for the reason usually given: an empty value and a null land in *different*
directories (`dt=` against `dt=__HIVE_DEFAULT_PARTITION__`) and are recorded distinctly in the log.
The value is lost when the column is reconstructed on the read — by delta-rs and by DuckDB's `delta`
extension alike, which is what makes it a property of the format rather than one engine's bug.
[../delta-lake-primer.md](../delta-lake-primer.md) has the full table.

**A value too long for the filesystem is a run-time failure, not a pre-flight refusal.** The cap is
255 bytes on the common local filesystems (object storage has no such limit) and it applies to the
**escaped** name: a partition value is percent-escaped into its directory name, and every escaped
byte costs three. Measured — a 90-character value of spaces is 90 bytes of UTF-8 and 270 once
escaped, and is refused although the value itself is a third of the cap. This one cannot be caught
before the write: it is delta-rs failing to create a file, translated when it surfaces. Measured —
`MergeErrorTests.A_partition_value_too_long_for_the_filesystem_is_reported_without_the_value` hands
over a 300-character value, `WriteBatchAsync` **succeeds**, and the error is thrown out of
`CommitAsync`.

So do not assume storage is untouched. delta-rs writes data files first and commits the log entry
last — this repository measured an orphan `part-*.snappy.parquet` left behind by the other write-time
failure it reproduces ([../troubleshooting.md](../troubleshooting.md)) — and on an `append` large
enough to have flushed a generation, that generation has already committed. What this repository has
*not* measured is exactly what a too-long partition value leaves behind; check the table's prefix
rather than assume.

An empty or null value in a column the table declares `NOT NULL` is the same code by the same
run-time route: delta-rs fails the insert, and only after the table exists.

Every offending column is named in one message. The messages name columns and never values.

## Small files

Partitioning multiplies files: every write touching *n* partitions produces at least *n* files, and
merges add more. Delta's answer is compaction (`OPTIMIZE`), and **this connector surfaces none** — pz
has no verb that maps to table maintenance. A table written only through this connector accumulates
files, and nothing here will compact them. See [../limitations.md](../limitations.md).
