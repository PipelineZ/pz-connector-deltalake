# Limitations

Two kinds of thing live here. **What this connector does not do** — capabilities it deliberately does
not have — and **measured behaviours you have to plan around**, each pinned to the version it was
observed against. Nothing in the second part is a general claim about Delta, about delta-rs or about
S3: it is what this connector measured, on a date, against a build, and every one of them is
reproduced by a test in this repository.

## What this connector does not do

### Table maintenance: no vacuum, no compaction, no checkpoint

`VacuumAsync`, `OptimizeAsync` and `CheckpointAsync` exist on the underlying table type and **none of
them is surfaced**. pz has no verb that maps to table maintenance, so this is deliberate rather than
overlooked — but it is a running cost you inherit:

- A `remove` action does not delete a file, so every merge and every `replace` leaves its predecessor
  on disk. Nothing here will ever clean that up.
- Merging repeatedly grows the file count, and partitioning multiplies it. Nothing here will compact.
- **No checkpoint is written.** Measured: a table taken to 13 commits with `DeltaLake.Net` 0.33.0 has
  13 JSON commit files in `_delta_log/`, no `.checkpoint.parquet`, and no `_last_checkpoint`. A
  long-lived table written only through this connector accumulates commit files indefinitely, and
  every reader replays all of them.

A table written through this connector must be maintained by whatever else you run against it, or not
at all.

### No change data feed, and no deletes

There is no `sync: {mode: cdc}` source. The `ApplyDeletes` capability is deliberately not declared, so
`on_delete: delete|soft` fails with **PZ0339** rather than half-working — the change-capture *source*
half does not exist, and a half-wired capability is worse than an absent one.

### No deletion vectors, column mapping, or change data feed on write

This connector writes `minReaderVersion: 1, minWriterVersion: 2` tables — measured — and there is no
option that turns any of the three optional features on. Reading a table that already uses one is
untested in either direction: reads go through DuckDB's `delta` extension, and what it supports is
DuckDB's business, not something measured here.

### `partition_by:` could not be declared through pz 0.2.2 — fixed in pz 0.3.0

pz 0.2.2 reads `partition_by:` as ONE column whose value substitutes calendar tokens in the sink's
`path:`, and refuses it with **PZ0219** when the path carries none. Delta partitions declaratively by
column value, with no templated path to route into, so **a Delta table written through pz 0.2.2 is
unpartitioned** — and a merge against it scans the whole table rather than the partitions the write
touches. That is worth 19–39× on a merge, measured, and it was the largest single gap between this
connector driven directly and this connector driven by pz.

pz 0.3.0 closes it ([coccor/pz#16](https://github.com/coccor/pz/pull/16)): `partition_by:` names the
columns an output is partitioned by, and `path:` decides who lays them out — calendar tokens mean pz
renders the layout, their absence means the destination records its own
(`ConnectorCapabilities.ColumnPartitionedWrites`). Verified against the released 0.3.0: the sample
declares `partition_by: ['dt']` and the table pz writes carries `"partitionColumns":["dt"]` in its
transaction log, with `dt=` directories on disk. Since this connector requires pz 0.3.0, the
paragraph above is history rather than a limitation.

No test in this repository writes a Delta table through pz with a calendar-templated `path:`; that
combination is neither covered nor meaningful for a store that partitions by column value.

### The external-connector path does not work against pz 0.2.2 — fixed in pz 0.3.0

A restored package does not load: pz's materializer extracts the wrong target framework and the wrong
RID out of a multi-targeted, multi-RID dependency, and never places a dependency's native assets where
the connector's load context probes. The causes are entirely in pz.
[installing.md](installing.md) has each one, what it looks like when it bites, and the script that
detects and stages around them.

pz 0.3.0 closes all of them ([coccor/pz#16](https://github.com/coccor/pz/pull/16)) — `pz.lock.json`
records each asset's archive path, native assets select through a RID graph, and a transitive
package's `native/` reaches the probe path. Against the released 0.3.0,
`scripts/verify-external-connector.sh` reports **no gaps** and passes under `PZ_VERIFY_STRICT=1`, the
mode that exists to go green exactly when this is fixed. This connector requires 0.3.0, so the
paragraph above applies only to a host too old to load it at all.

### Platform coverage

`DeltaLake.Net` ships `linux-x64`, `linux-arm64`, `osx-x64`, `osx-arm64` and `win-x64` — that is what
it SHIPS. **linux-x64 is the only platform anything here has been run on.** `linux-musl-x64` (Alpine)
does not work against pz 0.2.2, because it selects native assets by exact RID match with no RID-graph
fallback; pz 0.3.0 selects through the RID graph, so a musl host reaches `linux-x64` — untested here
like every other non-linux-x64 platform, but no longer refused by construction. `win-arm64` is not
shipped by `DeltaLake.Net` at all.

### Memory: `replace` and `merge` buffer the whole write

Their semantics are defined over the entire input, so the buffer *is* the write. Only `append` flushes
in bounded generations (`target_file_bytes`, default 128 MiB). A `replace` or `merge` of an output
larger than available memory is not something this connector can stage around.

### `append` is at-least-once

A crash between a successful commit and pz recording it duplicates rows on `pz retry`. Closing that
window needs a stable attempt identity, and pz 0.2.2's `OutputSpec` carries none — see
[concepts/delivery-guarantees.md](concepts/delivery-guarantees.md).

pz 0.3.0 adds `OutputSpec.Attempt` (`Node`, `Run`, `Ordinal`), which is exactly the identity that was
missing: a Delta commit can carry it as a commit property and the next attempt can read it back and
skip what already landed. **This connector has not implemented that** — the ABI now allows it, which
is a different statement from the window being closed, and `append` here is still at-least-once. Note
the promise is within-run only, so it would close the `pz retry` case above and not a second
`pz run`.

### Merges cannot see duplicates that are already in the table

Delta enforces no primary key. If the target already holds two rows for one key, a merge updates
**every** copy and leaves them all in place — measured; the write commits and reports success.
Detecting it would mean reading the whole target table on every merge, which costs more than the
operation it would protect. The same reach limit applies to the NULL/NaN key check: it reads the
batches this write hands over, not the table.

### Automatic merge pruning only applies when it is provably sound

A partition predicate is derived from the incoming rows only when **every** partition column is also a
merge key. Otherwise a row's partition value could change while its key did not, the derived predicate
would hide that row's current partition from the target scan, and the merge would insert a second
copy. The connector says why it skipped the derivation rather than staying silent.

### Azure `replace`, `merge` and concurrent commits are not proven

Not a claim that they fail — a statement that nothing here has run them. The emulator accepts one
commit per table through a generic endpoint, so there is no way to exercise a strategy that needs a
seeded target. `abfss://`-family roots are accepted and unit-tested at the classification level;
nothing has ever written a table through one. [compatibility.md](compatibility.md) is the matrix.

## Measured behaviours to plan around

### Concurrent writes to S3 are only as safe as the endpoint

**Measured 2026-08-22 against `DeltaLake.Net` 0.33.0, whose commits report `engineInfo: delta-rs:0.32.1`.**

A Delta commit to an `s3://` root is an object-store PUT with `PutMode::Create` — a conditional PUT,
carrying `If-None-Match` — and never a rename. Two consequences, both good:

- **No locking provider is needed.** A DynamoDB lock table is not required, not configured and not
  looked for.
- **`AWS_S3_ALLOW_UNSAFE_RENAME` is never set, under any configuration.** It is the option that turns
  a loud refusal into silently overwritten commits, and this connector does not have a code path that
  can set it. `ConcurrencyTests.No_s3_configuration_ever_asks_delta_rs_for_an_unsafe_rename` asserts
  that over every S3 config shape, on every leg, with no docker required.

The connector pins the mechanism explicitly (`AWS_CONDITIONAL_PUT=etag`) rather than inheriting a
library default, because a default is the kind of thing a version bump moves.

**What the connector cannot pin is your S3 endpoint.** A conditional PUT is a guarantee only where the
server enforces the precondition. Against a server that accepts the header and ignores it, concurrent
commits overwrite one another and **every writer reports success**. That is measured, not theoretical:

| Endpoint | 4 writers × 10 rounds | Commits reported successful | Commits that survived |
|---|---|---|---|
| MinIO `RELEASE.2025-09-07T16-13-09Z` | 40 concurrent commits | 40 | 40 |
| MinIO `RELEASE.2023-01-31T02-24-19Z` | 40 concurrent commits | 40 | **10** |

The 2023 build predates MinIO's support for conditional writes. It answered every `If-None-Match: *`
PUT with success, so each round's four writers all wrote the same commit file and three of them
vanished — no error, no warning, correct-looking row counts in the run report, and the rows simply not
there afterwards. The table above comes from a ten-round measurement run against each image in turn.
`S3ConcurrencyTests.Concurrent_appends_to_one_s3_table_never_lose_a_commit` is the standing assertion
that keeps it true — three rounds of the same four writers — and pointing its fixture at the 2023
image fails it, on the row count, in the first round.

**Amazon S3 itself.** AWS documents conditional writes on `PutObject` via `If-None-Match`, refusing
the second write with `412 Precondition Failed`
([S3 user guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)), and
announced the feature on 20 August 2024
([AWS What's New](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/)).
**This repository has never talked to Amazon S3** — every measurement above is against MinIO — so
that is AWS's claim, attributed, not a result established here. Verify it the same way you would
verify any other endpoint:

#### How to check whether your endpoint enforces `If-None-Match`

Two conditional PUTs of the same key. The first must succeed and the second must be refused. If the
second succeeds, concurrent Delta commits to that endpoint will be lost silently.

```bash
# aws-cli (works against any S3-compatible endpoint; drop --endpoint-url for Amazon S3)
aws s3api put-object --bucket YOUR_BUCKET --key pz-condput-probe \
    --if-none-match '*' --endpoint-url https://YOUR_ENDPOINT
aws s3api put-object --bucket YOUR_BUCKET --key pz-condput-probe \
    --if-none-match '*' --endpoint-url https://YOUR_ENDPOINT
```

- **Second call fails** with `PreconditionFailed` / HTTP 412 (`At least one of the pre-conditions you
  specified did not hold`) — the endpoint enforces the precondition. Concurrent writers are safe.
- **Second call succeeds** — the endpoint accepts the header and ignores it. **Do not run two writers
  against one Delta table on this endpoint.** Upgrade the store, or serialize your writes.

Delete `pz-condput-probe` afterwards. That is exactly the probe used to confirm MinIO
`RELEASE.2025-09-07T16-13-09Z`: first PUT accepted, second refused with *"At least one of the
pre-conditions you specified did not hold"*.

There is no way for this connector to run that check for you. The header is accepted either way, the
difference is only visible after a race has already lost a commit, and a probe would have to sign its
own request — impossible under the ambient-credential configurations this connector supports, where it
holds no keys at all.

**A single-writer table is unaffected**, whatever the endpoint.

### `replace` does not remove rows another writer committed while it was running

**Measured 2026-08-22 against `DeltaLake.Net` 0.33.0 (`engineInfo: delta-rs:0.32.1`), on local disk.**

A `strategy: replace` write commits an overwrite that removes the files its own snapshot listed, taken
when the write session opened. Rows another writer committed after that — and before this write
committed — are **not** removed, and no conflict is reported.

Worked example. A table holds ids 0–4. A replace session opens. Another writer appends ids 500–504.
The replace commits three rows, ids 100–102:

| ids in the table afterwards | |
|---|---|
| 100, 101, 102 | the replace's own rows |
| 500, 501, 502, 503, 504 | **the other writer's, still there** |
| — | 0–4, correctly removed |

The window is the whole write session, which for a large output is the whole run. If `replace` must
mean "the table holds exactly these rows", do not run two writers against that output at once.

### Run artifacts name the storage locations a failure touched

A write failure's message is written verbatim into `run_results.json` and onto the NDJSON event
stream, and it carries what the storage layer said — which includes the **bucket or container, the
object path, and the endpoint host and port**. A retry's `reason` carries the same text.

That is deliberate: a storage failure naming neither bucket nor path is undiagnosable. Credentials are
stripped before the message is built (`DeltaErrors.Redact`, covering both the `name=value` and the XML
shapes an S3 or Azure error arrives in), so what remains is operational identifiers rather than
secrets. But if you ship run artifacts or events off the machine — to a ticket, a log aggregator, a
support thread — **they name where your data lives**. Treat them accordingly.
