# Writing to a Delta table

Three strategies: `append`, `replace`, `merge`. All three go through the universal Arrow path —
DuckDB's delta extension is read-only, so there is no native-copy alternative on the write side.

## `merge`

`keys:` is required and names the columns that identify a row. The generated statement joins the
target and the incoming batch on those columns, updates the non-key columns of every row that matches,
and inserts every row that does not.

```yaml
outputs:
  orders:
    strategy: merge
    keys: [order_id]
    write:
      partition_by: [dt]
      merge_predicate: "target.dt >= '2026-01-01'"
```

`merge_predicate` narrows the merge further. It may only compare column names against literals: every
name must be qualified `target.` (a column of the table being written into) or `source.` (a column of
this output), and function calls, casts, `CASE` and subqueries are refused. That is a deliberate limit
— the predicate is interpolated into a statement handed to a SQL engine this connector does not own,
and the vocabulary that engine accepts is not one this connector can enumerate.

### Values a merge key cannot carry

A merge is refused, with `PZDL0405`, when a key column holds a value the join cannot match:

- **A null in any key column.** `target.k = source.k` against a null is null, never true, so the row
  matches nothing and is inserted a second time instead of updating the row it belongs to.
- **An empty string in a key that is also a partition column.** Delta writes a partition value into a
  directory name, where an empty string is indistinguishable from a null: the row reads back with null
  in that column, and the next merge duplicates it.

Both are refused rather than allowed to happen, because a duplicated row and a successful merge look
identical from the outside.

**Documented limit.** The null-key check reads the batches this write hands over; it does not read the
target table. A null key already sitting in the table that no incoming row touches is outside its
reach. Reading the whole target on every merge would cost more than the operation the check protects,
so it is not done.

### Values a partition column cannot carry

Independent of strategy — `append`, `replace` and `merge` alike — `PZDL0406` reports a partition value
this connector will not write:

- **An empty value in any partition column.** Refused before the batch is buffered. Delta writes a
  partition value into a directory name, and an empty value produces `col=`, which it cannot tell from
  a null. On a *nullable* partition column delta-rs therefore accepts the write and the rows read back
  with **null** in that column — a silent change to your data, with no error anywhere; on a `NOT NULL`
  one it fails the insert, but only after the table exists and after any generation an append already
  flushed has committed. Both string and binary columns are affected, and both are refused. A genuine
  null is *not* refused: a null written is a null read back.
- **A value whose directory name exceeds the filesystem's path-component limit** (255 bytes on the
  common local filesystems; object storage has no such limit). The value is percent-escaped on the way
  in, so a shorter string can still exceed it.

Every offending column is named in one message; the messages name columns and never values.

The guard's authority is the table's own partition columns, not `partition_by`. `partition_by` is
honoured only when the table is created, so a run against a table an earlier run partitioned need not
declare it.

## What a merge costs

Merge cost follows the partitions the write touches, not the size of the table — *provided the
partition columns are part of the join*, which is what happens whenever `partition_by` is a subset of
`keys`.

Measured with `MergeCostBench` (delta-rs 0.33.0 via DeltaLake.Net, local filesystem, 200 partitions,
1 000 source rows scattered across 5 of them, ids arranged so file statistics cannot prune on their
own; each figure the median of repeated runs after a discarded warm-up):

| table rows | partition column not joined | partition column joined | joined + derived `IN` list |
|---|---|---|---|
| 500 000 | 1 601 ms | 85 ms | 85 ms |
| 2 000 000 | 6 446 ms | 253 ms | 250 ms |
| 8 000 000 | 23 793 ms | 604 ms | 585 ms |

Two things follow, and the second was not what the design expected.

**Joining on the partition column is worth 19–38x.** That is the number to protect, and
`MergeSafetyTests.Merge_cost_follows_the_partitions_the_write_touches_not_the_table` is what protects
it (opt in with `PZDL_SLOW_TESTS=1`).

**The connector's own derived `IN` list is worth nothing measurable.** delta-rs constructs its own
early filter from the source's partition values whenever the partition column is joined — which is
exactly and only the case in which deriving one is sound. The derivation stays as a hedge against that
internal optimization changing, not because it is what makes a merge fast. A long list costs a little:
200 literals measured about 17% slower than none, since it is parsed and planned and prunes nothing
new. That, rather than any pruning cliff, is why the derivation caps out at 256 distinct values.

Reproduce with:

```bash
PZDL_SLOW_TESTS=1 dotnet test tests/Pz.Connector.DeltaLake.Tests -c Release \
  --filter "FullyQualifiedName~Merge_cost_follows"
```
