# Limitations

Observed facts, each pinned to the versions it was observed against. Nothing here is a general claim
about Delta, about delta-rs or about S3 — it is what this connector measured, on a date, against a
build, and every one of them is reproduced by a test in this repository.

## Concurrent writes to S3 are only as safe as the endpoint

**Measured 2026-08-22 against delta-rs 0.33.0 (DeltaLake.Net 0.33.0).**

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

**What to do about it.** Amazon S3 has enforced `If-None-Match` since August 2024, so a real AWS
endpoint is safe. If you write concurrently to an S3-compatible store — MinIO, Ceph, a vendor gateway
— check that it implements conditional writes, and upgrade it if it does not. There is no way for this
connector to detect a store that ignores the precondition: the header is accepted either way, and the
difference is only visible after a race has already lost a commit. **A single-writer table is
unaffected**, whatever the endpoint.

## `replace` does not remove rows another writer committed while it was running

**Measured 2026-08-22 against delta-rs 0.33.0, on local disk.**

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
