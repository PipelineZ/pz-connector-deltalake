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
and the vocabulary that engine accepts is not one this connector can enumerate. A predicate this
connector accepts but the SQL engine cannot parse is reported as `PZDL0107`.

### `merge_predicate` turns an excluded row into a DUPLICATE, not a skip

**Read this before using `merge_predicate`.** It is the sharpest edge on the merge surface, and it is
easy to read "narrows the merge" as "touches fewer rows".

The predicate is ANDed into the `ON` clause. A row it excludes is therefore **not matched** — and an
unmatched source row is an `INSERT`. The target row it should have updated stays exactly where it is,
and the table ends up with two rows for one key.

Worked example. Table `orders`, `keys: [id]`, `merge_predicate: "target.dt >= '2026-01-01'"`:

| | `id` | `dt` | `amt` |
|---|---|---|---|
| in the table | 1 | `2026-03-01` | 10 |
| in the table | 2 | `2025-11-04` | 20 |
| incoming | 1 | `2026-03-01` | 99 |
| incoming | 2 | `2025-11-04` | 99 |

The run succeeds and reports two rows written. Afterwards:

| `id` | `dt` | `amt` | |
|---|---|---|---|
| 1 | `2026-03-01` | 99 | updated — the predicate let it match |
| 2 | `2025-11-04` | 20 | the original, untouched |
| 2 | `2025-11-04` | 99 | **inserted: a second row for `id = 2`** |

No error, and the row count looks right. `MergeErrorTests.A_merge_predicate_may_name_a_column_only_the_table_has`
asserts exactly this shape — it is what proves the predicate ran.

**Do this instead.** Put the same condition on the SOURCE, in the pipeline's own SQL, so the rows the
merge must not touch never reach it:

```sql
select * from staged where dt >= '2026-01-01'
```

Use `merge_predicate` on top of that only to prune the target scan for speed — never as the only place
the condition appears. A condition that holds for the source and the target alike (`target.dt` matching
a filter the pipeline already applied) prunes without excluding anything, which is the safe shape.

**Why this connector does not turn the exclusion into a skip.** Delta's `MERGE` can guard the insert
branch as well (`WHEN NOT MATCHED AND (…)`), and delta-rs accepts one. Measured, for a predicate
written only over `source.` columns, that does give a true skip: the excluded row is neither updated
nor duplicated, and a genuinely new key is still inserted. But applying the same treatment to a
`target.`-qualified predicate — the form the example above uses, and the form anyone writing a
target-scan prune reaches for — is **worse than the duplicate**: a brand-new key has no target row, so
every `target.` reference in the guard is NULL, the guard is not true, and the row is **silently
dropped**. Measured: the same three-row source that duplicates one key under the plain form loses its
new key entirely under the guarded one. Losing a row without a word is a worse outcome than gaining
one that shows up in the table, so the connector emits the plain form for every predicate and this
page tells you the consequence instead.

### Values a merge key cannot carry

A merge is refused, with `PZDL0405`, when a key column holds a value that cannot match itself, so that
the row would be inserted a second time on every run instead of updating the row it belongs to. SQL has
exactly two such values:

- **A null in any key column.** `target.k = source.k` against a null is null, never true.
- **NaN in a float or double key column.** NaN never equals itself. (`-0.0` is fine and is not refused:
  `-0.0 = 0.0` is true, so such a row matches and updates in place.)

Both are refused rather than allowed to happen, because a duplicated row and a successful merge look
identical from the outside. An empty value in a partition column is the same hazard by a different
route and is refused separately — see below.

**Documented limit.** The key check reads the batches this write hands over; it does not read the
target table. A null or NaN key already sitting in the table that no incoming row touches is outside
its reach. Reading the whole target on every merge would cost more than the operation the check
protects, so it is not done.

## Values a partition column cannot carry

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

## `schema_policy: evolve`

`evolve` means the same thing on every strategy — `append`, `replace` and `merge` alike: a column the
pipeline produces that the table does not have is **added** to the table, and the rows already there
read null in it.

Under any other policy — including the default `fail_on_change` — a column the table lacks is refused
with `PZDL0301`.

**A column the write adds must be nullable.** Delta has no value to give it in rows committed before it
existed, and none can be invented. A `NOT NULL` addition is refused with `PZDL0301`, before anything is
written, on every strategy. The two workarounds are:

- declare the column nullable in the pipeline SQL (`cast(x as varchar)` on a `null` branch, a
  `try_cast`, or simply not marking it `not null`); or
- create a new table with the full schema and backfill into it.

The refusal stands **even when the table currently holds no rows**, where the addition would in fact
succeed. That is deliberate. Establishing emptiness means counting rows and then acting on the answer,
and Delta is a multi-writer format — another writer can add rows in between. Losing that race produces
exactly the outcome the rule exists to prevent, and it is a bad one: on `append` the write **commits
successfully and says nothing**, and every later read of the table fails with `Non-nullable column 'x'
is missing from the physical schema`. A false refusal costs one config edit; a wrong "it looked empty"
costs a table nobody can read.

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
