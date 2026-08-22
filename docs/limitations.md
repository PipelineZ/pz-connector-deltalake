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

**Amazon S3 itself.** AWS documents conditional writes on `PutObject` via `If-None-Match`, available
since August 2024. **This repository has never talked to Amazon S3** — every measurement above is
against MinIO — so that is AWS's claim, attributed, not a result established here. Verify it the same
way you would verify any other endpoint:

### How to check whether your endpoint enforces `If-None-Match`

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

## Run artifacts name the storage locations a failure touched

A write failure's message is written verbatim into `run_results.json` and onto the NDJSON event
stream, and it carries what the storage layer said — which includes the **bucket or container, the
object path, and the endpoint host and port**. A retry's `reason` carries the same text.

That is deliberate: a storage failure naming neither bucket nor path is undiagnosable. Credentials are
stripped before the message is built (`DeltaErrors.Redact`, covering both the `name=value` and the XML
shapes an S3 or Azure error arrives in), so what remains is operational identifiers rather than
secrets. But if you ship run artifacts or events off the machine — to a ticket, a log aggregator, a
support thread — **they name where your data lives**. Treat them accordingly.
