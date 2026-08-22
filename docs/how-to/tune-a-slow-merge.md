# Tuning a slow merge

A merge that takes seconds instead of milliseconds is almost always scanning the whole target table.
This page is the order to try things in, and the one option that will bite you if you reach for it
first.

## Why merges are slow

Parquet files are immutable, so updating one row rewrites the file it lives in. **Merge cost follows
the files the merge touches, not the rows it changes.** A 1,000-row source against an 8-million-row
table can rewrite most of the table if nothing narrows the target scan.

Measured with `MergeCostBench` (DeltaLake.Net 0.33.0, local filesystem, 200 partitions, 1,000 source
rows scattered across 5 of them, ids arranged so file statistics cannot prune on their own; each
figure the median of repeated runs after a discarded warm-up):

| table rows | partition column not joined | partition column joined |
|---|---|---|
| 500 000 | 1 601 ms | 85 ms |
| 2 000 000 | 6 446 ms | 253 ms |
| 8 000 000 | 23 793 ms | 604 ms |

**19–38×.** That is the number to protect.

## 1. Partition the table, and put the partition column in `keys:`

This is the whole game. When every `partition_by` column is also a merge key, the `ON` clause already
requires `target.p = source.p`, and the engine can eliminate every partition the write does not touch
without opening a file.

The connector will not derive a partition predicate when that condition does not hold, and it says why
rather than staying silent: a row's partition value can change while its key does not, and a predicate
derived anyway would hide that row's *current* partition from the target scan, the merge would see
NOT MATCHED, and it would insert a second copy. A silent duplicate is worse than a slow merge.

**Through pz this step is currently unreachable** — `partition_by:` cannot be declared (PZ0219). See
[partitioned-tables.md](partitioned-tables.md). It is the largest single gap between this connector
driven directly and this connector driven by pz.

## 2. Filter the source in the pipeline SQL

Fewer incoming rows means fewer target files touched. This costs nothing and cannot go wrong:

```sql
select * from {{ ref('staged') }} where dt >= '2026-01-01'
```

## 3. Only then, `merge_predicate:`

`merge_predicate` narrows the merge further, and it is the sharpest edge on this connector's surface.

**A row the predicate excludes is not skipped — it is INSERTED.** The predicate is ANDed into the `ON`
clause, so an excluded row is *unmatched*, and an unmatched source row is an insert. The target row it
should have updated stays where it is, and the table ends up with two rows for one key. The run
succeeds and the row count looks right.

Use it **only** as a target-scan prune on top of a condition the source already satisfies — a
predicate that is true for every row the pipeline emits prunes without excluding anything. Never as
the only place the condition appears.

The full worked example, and why the connector does not turn the exclusion into a skip (the guarded
form silently *drops* a brand-new key instead, which is worse), is in
[../reference/write.md](../reference/write.md).

## What is NOT worth tuning

**The connector's own derived `IN` list buys nothing measurable.** delta-rs constructs its own early
filter from the source's partition values whenever the partition column is joined — which is exactly
and only the case where deriving one is sound. Measured on the same three tables, with the list:
85, 250 and 585 ms, against the 85, 253 and 604 ms above without it — a difference inside run-to-run
noise. It stays as a hedge against that internal optimization changing, not because it is what makes
a merge fast. A long list even costs a little: 200 literals ran
about 17% slower than none, since it is parsed and planned and prunes nothing new. That, rather than
any pruning cliff, is why the derivation caps at 256 distinct values.

**`target_file_bytes` does not affect a merge.** It bounds an `append`'s flush size. `replace` and
`merge` are defined over their entire input and buffer all of it regardless.

**There is no `max_rows_per_group`.** It was measured to do nothing: 100,000 rows produce a single
Parquet row group of 100,000 rows whether the underlying setting is unset or set to 1,000. It is
deliberately not an option, so a run cannot be tuned with a setting that does not work.

## Reproducing the numbers

```bash
PZDL_SLOW_TESTS=1 dotnet test tests/Pz.Connector.DeltaLake.Tests -c Release \
  --filter "FullyQualifiedName~Merge_cost_follows"
```
