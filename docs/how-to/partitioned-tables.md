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
column is worth 19–38× on a merge.

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

**PZDL0406** refuses these before anything is written, on `append`, `replace` and `merge` alike:

- **An empty value.** A partition value becomes a directory name, and `col=` is what an empty string
  produces *and* what a null produces. On a nullable partition column delta-rs accepts the write and
  the rows read back with **null** — a silent change to your data. Both string and binary columns are
  affected. A genuine null is not refused: a null written is a null read back.
- **A value whose directory name exceeds the filesystem's path-component limit** — 255 bytes on the
  common local filesystems; object storage has no such limit. The value is percent-escaped on the way
  in, so a shorter string can still exceed it.

Every offending column is named in one message. The messages name columns and never values.

## Small files

Partitioning multiplies files: every write touching *n* partitions produces at least *n* files, and
merges add more. Delta's answer is compaction (`OPTIMIZE`), and **this connector surfaces none** — pz
has no verb that maps to table maintenance. A table written only through this connector accumulates
files, and nothing here will compact them. See [../limitations.md](../limitations.md).
