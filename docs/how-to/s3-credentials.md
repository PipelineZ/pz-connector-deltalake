# S3 credentials, endpoints, and multi-writer safety

An `s3://` (or `s3a://`) root turns on the S3 half of the connection block. The same options feed
both engines: DuckDB gets a scoped `CREATE SECRET`, delta-rs gets storage options.

## An explicit key pair

```yaml
lake:
  connector: deltalake
  root: s3://analytics/curated
  region: eu-west-1
  access_key_id: ${AWS_ACCESS_KEY_ID}
  secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

`access_key_id` and `secret_access_key` go together. Half a pair is refused with **PZDL0102** rather
than left to fall back to the ambient chain and fail much later as a permissions error naming nothing.

Add `session_token:` for temporary credentials.

## The ambient credential chain

Leave both out and the ambient chain is used: an instance profile, an IRSA/web-identity token, the
`AWS_*` environment. DuckDB is given `provider credential_chain`, delta-rs resolves its own — and any
`endpoint`, `region`, `url_style`, `use_ssl` or `session_token` you configured still reaches both.

```yaml
lake:
  connector: deltalake
  root: s3://analytics/curated
  region: eu-west-1
```

## An S3-compatible endpoint (MinIO, Ceph, and friends)

```yaml
lake:
  connector: deltalake
  root: s3://analytics/curated
  endpoint: minio.internal:9000
  url_style: path
  use_ssl: false
  access_key_id: ${MINIO_ACCESS_KEY}
  secret_access_key: ${MINIO_SECRET_KEY}
```

Three things worth knowing:

- **`endpoint` may be a bare `host[:port]` or a full URL.** The two engines disagree: DuckDB's secret
  wants a bare host, delta-rs's `AWS_ENDPOINT_URL` requires a scheme. The connector strips the scheme
  for one and adds it — chosen by `use_ssl` — for the other, so one value satisfies both.
- **`url_style: path` is what most S3-compatible stores need.** Left unset, neither engine is told
  anything and each keeps its own default.
- **`use_ssl: false` allows plain http.** For an emulator on localhost, not for anything else.

## Two connections to two buckets

Each connection's DuckDB secret is scoped to its own `root`, which matters more than it looks.
DuckDB does not pick a secret by name: it matches the path being read against each secret's scope
prefixes and, on a tie, picks alphabetically by secret name. Without scoping, a project with a prod
lake and a staging lake would have whichever name sorts first authenticate every Delta read in the
run — and it would surface as a bucket-level 403 naming nothing. Scoping is automatic; there is
nothing to configure.

## Concurrent writers: check your endpoint

This connector commits to S3 with a **conditional PUT** (`If-None-Match`), pinned explicitly rather
than inherited from a library default. Consequences:

- **No locking provider is needed.** A DynamoDB lock table is not required, not configured, and not
  looked for.
- **`AWS_S3_ALLOW_UNSAFE_RENAME` is never set, under any configuration.** It is the option that turns
  a loud refusal into silently overwritten commits, and this connector has no code path that can set
  it.

**What the connector cannot pin is your endpoint.** A conditional PUT is a guarantee only where the
server enforces the precondition. Against a server that accepts the header and ignores it, concurrent
commits overwrite one another and **every writer reports success**.

That is measured, not theoretical: against MinIO `RELEASE.2023-01-31T02-24-19Z`, 40 concurrent commits
all reported success and **10** survived. Against `RELEASE.2025-09-07T16-13-09Z`, all 40 survived.

AWS documents conditional writes on `PutObject` via `If-None-Match`
([S3 user guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)) and
announced the feature on 20 August 2024
([AWS What's New](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/)).
**This repository has never talked to Amazon S3** — every measurement here is against MinIO — so that
is AWS's claim, attributed, not a result established here.

[../limitations.md](../limitations.md) carries the two-command probe for checking your own endpoint,
and it takes about ten seconds. A single-writer table is unaffected whatever the answer.

## Environment variables that matter

| variable | effect |
|---|---|
| `AWS_S3_LOCKING_PROVIDER=dynamodb` | **breaks every S3 read and write** with PZDL0403 — it diverts commits to a log store this connector supplies no lock table for. Remove it. |
| `AWS_S3_ALLOW_UNSAFE_RENAME=true` | none. The write still commits through the conditional PUT. |
| `AWS_CONDITIONAL_PUT=disabled` | none. The connector sets this option explicitly, and a storage option beats the environment. |

All three measured on 2026-08-22, exported into the process environment. A shell cannot force this
connector onto an unsafe commit path; the one thing a shell can do is break it loudly, and that is the
first row.

## What run artifacts will contain

A write failure's message reaches `run_results.json` and the NDJSON event stream verbatim, and it
carries what the storage layer said — **the bucket or container, the object path, and the endpoint
host and port**. Credentials are stripped before the message is built, covering both the `name=value`
and the XML shapes an S3 error arrives in, so what remains is operational identifiers rather than
secrets. But if you ship run artifacts off the machine, they name where your data lives.
