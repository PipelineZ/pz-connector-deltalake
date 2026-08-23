# Writing to a Delta table

Three strategies: `append`, `replace`, `merge`. All three go through the universal Arrow path —
DuckDB's delta extension is read-only, so there is no native-copy alternative on the write side.

A write is declared as `sink()` keyword arguments in the pipeline's own SQL, or under
`entities: <e>: write:` in `connections.yml` — never both, and never merged. The names are identical
on both surfaces.

```sql
INSERT INTO {{ sink('lake', 'orders', strategy: 'merge', keys: ['id']) }}
select id, dt, amount from {{ ref('stg_orders') }}
```

```yaml
lake:
  connector: deltalake
  root: /srv/lake
  entities:
    orders:
      write:
        strategy: merge
        keys: [id]
```

`strategy`, `keys`, `duplicates`, `on_delete`, `schema_policy` and `retry` are pz's own names; pz
reads them itself and this connector never sees them. Everything else is a connector write option,
and an unrecognized one is **PZDL0108** with the nearest known name suggested — never a silently
ignored setting. Every problem with a write is reported at once.

## Options

| option | type | default | notes |
|---|---|---|---|
| `path` | string | the output's own name | The table's location under `root:`. An absolute path, or one carrying its own scheme, ignores `root:`. |
| `partition_by` | list of column names | `[]` (unpartitioned) | Honoured only when the table is CREATED; a run against an already-partitioned table need not repeat it, and one that declares a different set than the table has is refused with PZDL0301. Must be a list — a bare string is PZDL0108, because a `partition_by: dt` quietly read as "no partitioning" would be a silent and permanent layout change. **Not declarable through pz 0.2.2** — see below; pz 0.3 declares it as a list, unchanged from the spelling here. |
| `merge_predicate` | string | none | `merge` only. Narrows the merge's target scan. Read the section below before using it: a row it excludes is DUPLICATED, not skipped. |
| `target_file_bytes` | integer > 0 | `134217728` (128 MiB) | `append` only: the buffered size at which an append flushes a generation, so memory tracks a file rather than the whole write. `replace` and `merge` are defined over their entire input and buffer all of it whatever this says. A value of zero or less is PZDL0108. |

`max_rows_per_group` is deliberately **not** an option. Measured against DeltaLake.Net 0.33.0:
writing 100,000 rows produces a single Parquet row group of 100,000 rows whether the underlying
`MaxRowsPerGroup` is left unset or set to 1,000 (counted with DuckDB's `parquet_metadata`). The
value does reach Rust — 0 aborts the write with `assertion failed: step != 0` — it simply has no
effect on the output. Accepting it would make it a validated no-op that reads like a working
setting; leaving it out makes it an honest "unknown write option" instead. It comes back the day it
demonstrably shapes a row group.

## `merge`

`keys:` is required and names the columns that identify a row. The generated statement joins the
target and the incoming batch on those columns, updates the non-key columns of every row that matches,
and inserts every row that does not.

```yaml
lake:
  connector: deltalake
  root: /srv/lake
  entities:
    orders:
      write:
        strategy: merge
        keys: [order_id]
        merge_predicate: "target.dt >= '2026-01-01'"
```

That example omits `partition_by:` on purpose — pz 0.2.2 refuses it here (PZ0219), for the reason in
"`partition_by:` cannot be declared through pz today" below. Driven directly, it would sit beside
`merge_predicate` in the same block.

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

**A rejected predicate can still leave the table wider.** The check this connector applies to
`merge_predicate` is a lexical allowlist, not a SQL parser, so a predicate it accepts can still be
rejected by the engine's own parser — reported as `PZDL0107`, naming the option. Under
`schema_policy: evolve`, if that same run also added a column, the column has already been committed
by the time the statement is parsed, so the run fails without writing a row and the table is left one
nullable column wider. delta-rs offers no dry-run for a merge statement, so there is nothing to check
against beforehand. Nothing is lost — the added column is nullable — but a later run under
`fail_on_change` will then be refused for a column it does not produce.

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

### The same key twice in one write

A write may name the same merge key more than once — a pipeline's `SELECT` is not deduplicated, so it
is an ordinary input shape. **The last row for a key is the one that lands.** Rows are resolved to one
per key before the `MERGE` statement runs, so the table ends up with exactly one row per key regardless
of how many the write carried.

This is the merge contract the rest of pz implements (its postgres and sql server sinks resolve the
same repeat in SQL), and it is not something a raw Delta `MERGE` can express. Its `ON` clause matches
the *target* against the source, so two source rows for one key are two independent matches: if the
key is already in the table delta-rs refuses the whole statement, and if it is **not**, both rows fall
to `WHEN NOT MATCHED` and both are inserted — one commit, no error, and a duplicate of a key the output
declared unique.

`PZDL0402` therefore no longer reports anything you can fix. It maps delta-rs's own
"multiple source rows" error, which describes a repeat in the *incoming* rows — and those are resolved
before the statement runs, so reaching it means this connector's idea of key equality disagreed with
the SQL engine's. If you ever see it, it is a connector bug: report it with the merge key column names
and their types. There is no configuration change that avoids it.

**Documented limit: a duplicate already in the table is not detected, and not repaired.** If the table
already holds two rows for one key — left by an older run, by a writer that is not pz, or by an
`append` — a merge updates **every** copy and leaves them all in place. Measured against
`DeltaLake.Net` 0.33.0: the write commits, reports success, and the duplicate survives with the new
values in both
rows. Nothing here catches it, and `PZDL0402` does not fire for it. Detecting it would mean reading the
whole target table on every merge, which costs more than the operation it would protect, so the
connector does not. If you suspect duplicates in a table, check it directly.

**`PZDL0305`** refuses a merge key whose type cannot be compared row to row — a list, struct or map
column. Without a comparison there is no way to tell a repeat from two distinct keys, and an unresolved
repeat is a silently duplicated row rather than an error, so the key is refused at the start of the
write instead. Use a scalar key — a number, string, boolean, date or timestamp — and derive it in the
pipeline SQL if the source column is nested.

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

- **An empty value in any partition column.** Refused before the batch is buffered. An empty
  partition value does not survive the round trip: on a *nullable* partition column the write is
  accepted and the rows read back with **null** in that column — a silent change to your data, with
  no error anywhere — and on a `NOT NULL` one delta-rs refuses the write for holding a null it never
  wrote, but only after the table exists and after any generation an append already flushed has
  committed. Both string and binary columns are affected, and both are refused. A genuine null is
  *not* refused: a null written is a null read back.

  The mechanism is not the one usually given. On disk and in the log the two are **distinct** —
  measured: an empty value lands in `dt=` with `"partitionValues":{"dt":""}`, a null in
  `dt=__HIVE_DEFAULT_PARTITION__` with `{"dt":null}`. The value is lost when the column is
  reconstructed on the READ, and by both delta-rs and DuckDB's `delta` extension alike.
- **A value whose directory name exceeds the filesystem's path-component limit** (255 bytes on the
  common local filesystems; object storage has no such limit). A partition value is percent-escaped
  into its directory name, and every escaped byte costs three — measured: `a/b` becomes `dt=a%2Fb`,
  `café` becomes `dt=caf%C3%A9`, and a 90-character value of spaces is 90 bytes of UTF-8 and 270 once
  escaped, which is refused although the value is a third of the cap. **Not a pre-flight refusal**,
  unlike the bullet above: it is delta-rs failing to create a file, surfaced when it happens —
  measured, `WriteBatchAsync` succeeds and the error comes out of `CommitAsync`.

Every offending column is named in one message; the messages name columns and never values.

The guard's authority is the table's own partition columns, not `partition_by`. `partition_by` is
honoured only when the table is created, so a run against a table an earlier run partitioned need not
declare it.

### `partition_by:` cannot be declared through pz 0.2.2

pz 0.2.2 reads `partition_by:` as ONE column whose value substitutes calendar tokens
(`{yyyy}/{MM}/{dd}`) in the sink's `path:`, and refuses the option with `PZ0219` when the path carries
no such tokens. Delta partitions declaratively by column value, with no templated path to route into,
so a Delta table created through pz 0.2.2 is **unpartitioned** — this option, and everything below
that depends on it, is reachable only when the connector is driven directly. `installing.md` has the
detail, and pz 0.3 closes it ([coccor/pz#16](https://github.com/coccor/pz/pull/16)).

## `schema_policy: evolve`

`evolve` means the same thing on every strategy — `append`, `replace` and `merge` alike: a column the
pipeline produces that the table does not have is **added** to the table, and the rows already there
read null in it.

Under any other policy — including the default `fail_on_change` — a column the table lacks is refused
with `PZDL0301`.

**A column that `append` or `merge` adds must be nullable.** Delta has no value to give it in rows
those strategies leave in place, and none can be invented. A `NOT NULL` addition is refused with
`PZDL0301`, before anything is written. Three workarounds:

- declare the column nullable in the pipeline SQL (`cast(x as varchar)` on a `null` branch, a
  `try_cast`, or simply not marking it `not null`);
- create a new table with the full schema and backfill into it; or
- use `strategy: replace` — but know what it does first (below).

`replace` is exempt from the rule, because it is one **total** overwrite: every active file is
rewritten, including partitions this run does not touch, so no row survives that predates the column.
That is also the reason it is a workaround to reach for with care — `replace` **discards every row the
table holds** and rewrites it from this run's data.

Two columns whose names differ only in case cannot both exist, on any strategy: Delta rejects the
pair, and the write is refused with `PZDL0301` naming both spellings.

The `NOT NULL` refusal stands **even when the table currently holds no rows**, where the addition would
in fact succeed. That is deliberate. Establishing emptiness means counting rows and then acting on the answer,
and Delta is a multi-writer format — another writer can add rows in between. Losing that race produces
exactly the outcome the rule exists to prevent, and it is a bad one: on `append` the write **commits
successfully and says nothing**, and every later read of the table fails with `Non-nullable column 'x'
is missing from the physical schema`. A false refusal costs one config edit; a wrong "it looked empty"
costs a table nobody can read.

## What a merge costs

Merge cost follows the partitions the write touches, not the size of the table — *provided the
partition columns are part of the join*, which is what happens whenever `partition_by` is a subset of
`keys`. Through pz that condition cannot be met at all, because `partition_by` cannot be declared
(above); these figures describe the connector driven directly.

Measured with `MergeCostBench` (DeltaLake.Net 0.33.0, local filesystem, 200 partitions,
1 000 source rows scattered across 5 of them, ids arranged so file statistics cannot prune on their
own; each shape timed once after a discarded warm-up, and the three table sizes taken from separate
invocations of that bench — the committed regression runs only the 2 000 000-row size, and asserts
ratios rather than these figures):

| table rows | partition column not joined | partition column joined | joined + derived `IN` list |
|---|---|---|---|
| 500 000 | 1 601 ms | 85 ms | 85 ms |
| 2 000 000 | 6 446 ms | 253 ms | 250 ms |
| 8 000 000 | 23 793 ms | 604 ms | 585 ms |

Two things follow, and the second was not what the design expected.

**Joining on the partition column is worth 19–39x** — 1 601/85, 6 446/253, 23 793/604. That is the
number to protect, and
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
