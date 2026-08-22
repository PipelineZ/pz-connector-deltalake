# Delta Lake, for a data engineer who has never used it

Delta Lake is a **protocol**, not a file format. That one sentence explains almost everything
surprising about this connector: why a directory full of Parquet is not readable as Parquet, why a
merge costs what it costs, why two writers can conflict, why deleted files linger, and why
`partition_by` is the single biggest lever on write cost.

This page is the protocol. It assumes you know ETL and Parquet, and nothing about Delta.

The protocol itself is specified in the
[Delta Transaction Log Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md), which is
the authority for everything described here as protocol behaviour — action types, reader and writer
versions, table features, partition value serialization. This page is the working subset, plus what
this connector actually does.

**Every log excerpt below was produced by this connector's own stack on 2026-08-22** — `DeltaLake.Net`
0.33.0, whose commits report `engineInfo: delta-rs:0.32.1`, on local disk. The JSON is verbatim apart
from line wrapping and a few elided timestamps; the files themselves hold one JSON object per line.

## 1. What a Delta table is

A Delta table is a **directory**:

```
orders/
  _delta_log/
    00000000000000000000.json
    00000000000000000001.json
    00000000000000000002.json
  part-00000-55333a76-…-c000.snappy.parquet
  part-00000-fb8050f9-…-c000.snappy.parquet
```

Ordinary Parquet files, plus a transaction log. And the log — **not** the directory listing — is what
defines the table's contents.

Two consequences hit immediately:

- **A Delta directory is not readable as plain Parquet.** Point a Parquet reader at that glob and it
  will happily read both files. In the table above, only the second is live: the first was superseded
  by the merge in commit 2 and is not part of the table any more. A plain-Parquet read returns rows
  that the table does not contain.
- **Removing a data file by hand corrupts the table.** The log names files; it has no way to notice
  that one is gone. A reader replaying the log will ask for a file that is not there and fail.

Everything a Delta reader does starts by reading `_delta_log/`.

## 2. The log

Commits are numbered files: `00000000000000000000.json`, then `…001.json`, and so on. Each is a
sequence of **actions**, one JSON object per line. The action types that matter:

| action | what it says |
|---|---|
| `protocol` | the minimum reader and writer versions an engine needs to touch this table |
| `metaData` | the schema, the partition columns, the table id, table configuration |
| `add` | a data file is now part of the table — with its path, partition values, size and statistics |
| `remove` | a data file is no longer part of the table (the file itself is not deleted) |
| `txn` | an application-supplied transaction id, for writers that use one to make a commit idempotent |
| `commitInfo` | provenance: which operation, which engine, what it measured. Advisory, not semantic. |

A reader builds the table's state by **replaying** every commit in order: apply the `metaData` it
finds, add each `add`, drop each `remove`. What is left is the set of live files — the snapshot.

### A real three-commit table

Schema `id bigint not null, dt string not null, amt double`. Commit 0 creates it, commit 1 appends
two rows, commit 2 merges two rows in (updating `id = 2`, inserting `id = 3`).

**Commit 0 — create.** `commitInfo` elided; the two actions that matter:

```json
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"e6b0c1ee-…","format":{"provider":"parquet","options":{}},
  "schemaString":"{\"type\":\"struct\",\"fields\":[
     {\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},
     {\"name\":\"dt\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},
     {\"name\":\"amt\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}",
  "partitionColumns":[],"createdTime":…,"configuration":{}}}
```

Rows in the table after commit 0: **none**. There is no `add`, so the table is empty — it exists,
with a schema, and holds nothing.

**Commit 1 — append two rows.**

```json
{"commitInfo":{"operation":"WRITE","operationParameters":{"mode":"Append"},
  "engineInfo":"delta-rs:0.32.1",
  "operationMetrics":{"num_added_files":1,"num_added_rows":2,"num_removed_files":0,…}}}
{"add":{"path":"part-00000-55333a76-…-c000.snappy.parquet","partitionValues":{},"size":1081,
  "dataChange":true,
  "stats":"{\"numRecords\":2,
     \"minValues\":{\"amt\":10.5,\"dt\":\"2026-01-01\",\"id\":1},
     \"maxValues\":{\"dt\":\"2026-01-02\",\"id\":2,\"amt\":20.25},
     \"nullCount\":{\"dt\":0,\"id\":0,\"amt\":0}}"}}
```

Rows after commit 1:

| `id` | `dt` | `amt` |
|---|---|---|
| 1 | 2026-01-01 | 10.5 |
| 2 | 2026-01-02 | 20.25 |

**Commit 2 — merge.** One row updated, one inserted. Note that a merge does not edit a file:

```json
{"commitInfo":{"operation":"MERGE",
  "operationParameters":{"predicate":"id >= 2 AND id <= 3",
    "mergePredicate":"target.id = source.id",
    "matchedPredicates":"[{\"actionType\":\"update\"}]",
    "notMatchedPredicates":"[{\"actionType\":\"insert\"}]"},
  "readVersion":1,
  "operationMetrics":{"num_output_rows":3,"num_source_rows":2,
    "num_target_files_scanned":1,"num_target_files_skipped_during_scan":0,
    "num_target_rows_copied":1,"num_target_rows_updated":1,"num_target_rows_inserted":1,…}}}
{"add":{"path":"part-00000-fb8050f9-…-c000.snappy.parquet","partitionValues":{},"size":1134,
  "dataChange":true,
  "stats":"{\"numRecords\":3,\"minValues\":{\"amt\":10.5,\"id\":1,\"dt\":\"2026-01-01\"},
     \"maxValues\":{\"amt\":99.0,\"dt\":\"2026-01-03\",\"id\":3},
     \"nullCount\":{\"dt\":0,\"id\":0,\"amt\":0}}"}}
{"remove":{"path":"part-00000-55333a76-…-c000.snappy.parquet","dataChange":true,
  "deletionTimestamp":…,"partitionValues":{},"size":1081}}
```

Rows after commit 2:

| `id` | `dt` | `amt` | |
|---|---|---|---|
| 1 | 2026-01-01 | 10.5 | copied into the new file unchanged |
| 2 | 2026-01-02 | 99.0 | updated |
| 3 | 2026-01-03 | 30.0 | inserted |

Three things to take from that commit, because each one explains a cost you will meet:

- **A one-row update rewrote a whole file.** `num_target_rows_copied: 1` is the row that had nothing
  to do with the merge and was rewritten anyway, because Parquet files are immutable. This is why
  merge cost follows the files the merge touches, not the rows it changes.
- **`readVersion: 1`** records the snapshot the writer planned against. That is the basis of conflict
  detection (§8).
- **`predicate`** is a filter the writer derived for itself from the incoming rows, to narrow the
  target scan. Narrowing that scan is where merge performance lives — see
  [how-to/tune-a-slow-merge.md](how-to/tune-a-slow-merge.md).

### What a `MERGE` joins — and what Delta does not enforce

Two protocol facts sit behind that commit, and both surprise people arriving from a database.

**Delta enforces no primary key.** There is no uniqueness constraint anywhere in the format — the log
declares a schema and partition columns, and nothing else. A Delta table can hold two rows with the
same `id` and be perfectly valid. Nothing detects it, nothing repairs it, and a merge will not tell
you: measured against `DeltaLake.Net` 0.33.0, a merge into a table already holding two rows for one
key **commits, updates both copies, and leaves the duplicate in place**. If you need a key to be
unique, you are the one enforcing it.

**A `MERGE` matches the target against the source.** Look at the commit's own
`mergePredicate: "target.id = source.id"`. Every target row is tested against the incoming rows —
which is not the same thing as "each incoming row is applied once". Two incoming rows carrying the
same key are two independent matches, and Delta has no rule for which of them wins:

- if that key is **already in the table**, delta-rs refuses the whole statement — "multiple source
  rows";
- if it is **not**, both rows fall to `WHEN NOT MATCHED` and **both are inserted**. One commit, no
  error, and a duplicate of a key you thought was unique.

That is the raw protocol behaviour. This connector does not leave you with it: it resolves an
incoming key named twice to its last row before the statement runs, so a write carrying a repeat
lands exactly one row for it. But the reason that resolution has to exist is entirely in the two
paragraphs above, and it is why a merge cannot clean up duplicates that are already there.

## 3. Versions and time travel

Every commit is a version. Version *N* is the table you get by replaying commits `0 … N` and stopping
— nothing more exotic than that. Version 1 of the table above holds two rows; version 2 holds three.

That is exactly what `read: { version: N }` does:

```yaml
lake:
  connector: deltalake
  root: /srv/lake
  entities:
    orders:
      read:
        version: 1
```

Two limits fall straight out of the mechanism. Time travel can only reach versions **still in the
log**, and it can only be read while the files those versions named **still exist** (§4). See
[how-to/time-travel.md](how-to/time-travel.md).

### Checkpoints

Replaying thousands of JSON commits is slow, so the protocol allows a **checkpoint**: a Parquet
summary of the state at some version, letting a reader start there instead of at commit 0.

**This writer does not appear to make one on its own.** Measured: a table taken to 13 commits with
`DeltaLake.Net` 0.33.0 has 13 JSON commit files in `_delta_log/`, no `.checkpoint.parquet`, and no
`_last_checkpoint`. The underlying library exposes a checkpoint call; this connector does not surface
it, because pz has no verb that maps to table maintenance. A long-lived table written only through
this connector therefore accumulates commit files indefinitely.

## 4. `remove` does not delete anything

Look again at the directory listing in §1: **both** Parquet files are still there, after commit 2
removed the first one. That is measured, not incidental — a `remove` action makes a file invisible to
the table's current version, and leaves the bytes exactly where they are.

That is what makes time travel possible at all. It is also a bill:

- Every rewrite — every merge, every `replace` — leaves its predecessor on disk.
- A table merged into daily grows by roughly the rewritten data each day, whatever its row count does.

The protocol's answer is **VACUUM**: delete files that no live version references and that are older
than a retention window. The retention window is the whole subtlety:

- Vacuum aggressively and **time travel breaks** — a version whose files were deleted cannot be
  replayed any more.
- Vacuum aggressively and **in-flight readers break** — a reader that resolved a snapshot minutes ago
  is still reading files by name, and deleting them out from under it is a read failure.

**This connector exposes no vacuum.** pz has no maintenance verb to hang it on, so it is deliberately
not surfaced; a table this connector writes must be vacuumed by whatever else you use against it, or
not at all. That is a real operating cost and it is listed in
[limitations.md](limitations.md).

## 5. Partitioning

A partitioned table puts each partition's files in a directory named for its value:

```
orders_p/
  _delta_log/
  dt=2026-01-01/part-00000-26833c47-…-c000.snappy.parquet
  dt=2026-01-02/part-00000-a8165043-…-c000.snappy.parquet
```

`metaData` declares them, once, when the table is created:

```json
{"metaData":{…,"partitionColumns":["dt"],…}}
```

and every `add` repeats its own file's values:

```json
{"add":{"path":"dt=2026-01-01/part-00000-26833c47-…-c000.snappy.parquet",
  "partitionValues":{"dt":"2026-01-01"},"size":782,"dataChange":true,
  "stats":"{\"numRecords\":1,\"minValues\":{\"amt\":10.5,\"id\":1},
     \"maxValues\":{\"id\":1,\"amt\":10.5},\"nullCount\":{\"amt\":0,\"id\":0}}"}}
```

### Partition columns are not stored in the data files

Read that `add` again: `dt` appears in `partitionValues` and in the *path*, and it is absent from the
statistics. It is absent from the Parquet file too. Measured — the columns of that exact file, read
with DuckDB's `parquet_schema`:

```
id  : INT64
amt : DOUBLE
```

No `dt`. The engine reconstructs the column from the directory name.

Three practical consequences:

- **Pruning a partitioned scan is free.** An engine filtering on `dt` does not open a single file to
  decide which ones to skip; it reads `partitionValues` out of the log. This is why joining on the
  partition column is the biggest single lever on merge cost — measured at **19–39×** in
  [reference/write.md](reference/write.md).
- **A partition value is a directory name, and inherits that layer's limits.** A component longer than
  the filesystem allows (255 bytes on the common local filesystems) fails, and the value is
  percent-escaped on the way in, so a shorter string can still exceed it.
- **An empty partition value is indistinguishable from a null.** `dt=` is what an empty string
  produces, and it is what a null produces. Written to a nullable partition column, empty strings
  read back as **null** with no error anywhere. This connector refuses that write outright
  (PZDL0406) rather than let it change your data quietly.

### An overwrite is total, not per-partition

If you arrive from Spark's dynamic partition overwrite, this is the one to read twice. An overwrite
in Delta replaces the **whole table**, not the partitions the incoming data happens to touch. Every
active file is removed and rewritten from what this write carries, partitions this run never
mentioned included.

Measured: on a table partitioned by `dt` with rows in two partitions, an overwrite writing only the
first leaves the second partition's row **gone**.

The protocol does allow a scoped overwrite — a `replaceWhere` predicate that confines it to matching
rows. **This connector never sets one**, on any strategy, and that is load-bearing rather than
incidental: `strategy: replace` is the one strategy allowed to add a `NOT NULL` column to a table,
and that exemption is only sound while no row can survive the write. A partial overwrite would leave
rows behind that predate the column.

So `strategy: replace` means "the table holds exactly what this run produced". Time travel is
unaffected — an older version still carries its own files and its own schema.

## 6. Statistics, and why they sometimes do nothing

Every `add` carries per-file statistics:

```json
"stats":"{\"numRecords\":2,
   \"minValues\":{\"amt\":10.5,\"dt\":\"2026-01-01\",\"id\":1},
   \"maxValues\":{\"dt\":\"2026-01-02\",\"id\":2,\"amt\":20.25},
   \"nullCount\":{\"dt\":0,\"id\":0,\"amt\":0}}"
```

An engine reading `where id > 1000` consults `maxValues.id` for each file and skips the ones that
cannot contain a match — **without opening them**. `numRecords` answers a `count(*)` from the log
alone.

The catch is that statistics prune only when the data's *layout* correlates with the predicate.

- **A key correlated with file layout prunes well.** Rows written in `id` order give each file a
  narrow `[min, max]`, and a predicate on `id` eliminates most files immediately.
- **A UUID key prunes nothing.** Every file's range covers nearly the whole key space, so every file
  is a candidate and the engine reads all of them.

This is not a footnote — it is the difference between a fast merge and a slow one, and it is a trap
for benchmarks. Contiguous synthetic keys let file statistics prune almost perfectly on their own,
which is a best case nobody's data matches: a benchmark built that way measures the fixture, not the
merge. The cost table in [reference/write.md](reference/write.md) is measured on a fixture that
deliberately scatters keys — a partition holds every 200th id, so no file's id range is narrow — for
exactly this reason. If your own keys happen to be correlated with write order, expect to beat those
numbers; if they are UUIDs, expect not to.

## 7. Protocol versions

The `protocol` action gates who may touch the table:

```json
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
```

That is what this connector's stack writes — measured, on both the plain and the partitioned table
above.

`minReaderVersion` is the floor for reading, `minWriterVersion` the floor for writing. Newer protocol
features raise them: deletion vectors, column mapping and the rest are gated this way, which is how a
table stays safe from an engine too old to understand what is in it. The practical consequence runs
one way: **a table a newer engine wrote may be unreadable by an older one**, and the older engine
should refuse rather than misread it. Nothing stops a *newer* engine from reading an old table.

Higher versions are also expressed as named **table features** rather than a single number in current
Delta revisions. This connector writes neither: it writes reader 1 / writer 2, which is the widest
compatibility available.

## 8. Concurrency: optimistic, and only as safe as the store

There is no lock. A writer:

1. reads the current version — `readVersion: 1` in the merge above;
2. does its work, planning against that snapshot;
3. tries to write commit file `N+1`.

Step 3 must be **put-if-absent**: it succeeds only if `00000000000000000002.json` does not already
exist. If another writer got there first, this writer's commit fails, and it either retries against
the new snapshot or reports a conflict. In pz, a lost race is **PZDL0401**, classified transient, so
the engine's own retry policy decides what happens next. Commit conflicts are normal.

That whole design rests on the store being able to say "this key already exists" atomically.

- **A POSIX filesystem** can: an atomic create.
- **An object store** historically could not. A plain S3 `PutObject` overwrites, which is why older
  Delta stacks needed an external lock table (DynamoDB) to serialize commits — that history is
  delta-rs's and AWS's, not something this repository measured.
- **Conditional writes restore put-if-absent.** AWS documents `If-None-Match` on `PutObject`,
  refusing the second write with `412 Precondition Failed`
  ([S3 user guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)),
  announced 20 August 2024
  ([AWS What's New](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/)).
  **No test in this repository has ever talked to Amazon S3** — everything measured below is MinIO —
  so AWS's half of this is attributed, not established here. This connector commits with a
  conditional PUT and pins the mechanism explicitly rather than inheriting a library default. It
  never sets `AWS_S3_ALLOW_UNSAFE_RENAME`, the option that trades a loud refusal for silently
  overwritten commits, and it needs no lock table.

**What goes wrong without it is silent.** An endpoint that accepts `If-None-Match` and ignores it lets
every writer succeed and lets the last one win. Measured, 4 writers × 10 rounds against two MinIO
builds:

| endpoint | commits reported successful | commits that survived |
|---|---|---|
| MinIO `RELEASE.2025-09-07T16-13-09Z` | 40 | 40 |
| MinIO `RELEASE.2023-01-31T02-24-19Z` | 40 | **10** |

No error, no warning, correct-looking row counts in the run report, and three quarters of the rows
simply not there. [limitations.md](limitations.md) carries a two-command probe for checking your own
endpoint, and [how-to/s3-credentials.md](how-to/s3-credentials.md) covers the configuration side.

## 9. Features this connector does not write

Delta has grown a set of optional protocol features. Knowing the names is worth a minute, because you
may meet a table that uses one.

- **Deletion vectors.** Instead of rewriting a file to remove a few rows, a writer records a bitmap of
  deleted row positions beside it. Cheap deletes, more work for readers.
- **Column mapping.** Decouples the logical column name from the Parquet field name, so a column can
  be renamed or dropped without rewriting data.
- **Change data feed.** Persists per-row change records so a downstream consumer can read what changed
  between two versions rather than diffing snapshots.

**This connector writes none of them**, and does not enable them on a table it creates. It writes
reader 1 / writer 2 tables, and there is no option here that turns any of the three on.

Reading a table that *already* uses one is a different question, and the honest answer is that it is
untested: no test in this repository writes or reads a table with deletion vectors, column mapping or
a change data feed. Reads go through DuckDB's `delta` extension, whose support for those features is
DuckDB's business and not something measured here. See [compatibility.md](compatibility.md).

## 10. Where this connector sits

- **Reads** are DuckDB's `delta_scan` — log replay, file skipping and Parquet scanning all happen
  inside DuckDB, and rows never enter .NET.
- **Writes** are delta-rs, because DuckDB's delta extension is read-only.

That split, and what it means when something goes wrong on your platform, is
[concepts/architecture.md](concepts/architecture.md).
