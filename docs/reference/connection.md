# The `deltalake` connection

A connection is a **place with credentials**. For this connector the place is the directory a Delta
table lives under — a local path, an S3 bucket prefix, or an Azure Blob/ADLS container prefix — and
the credentials are whatever that place needs.

```yaml
lake:
  connector: deltalake
  root: /srv/lake
```

One connection, two translations. The read path is a DuckDB `delta_scan`, so credentials become a
DuckDB `CREATE SECRET`; the write path is delta-rs, so the same credentials become delta-rs storage
options. Both are built from the options below and neither is ever logged: no credential value
reaches a planner reason string, `plan.json`, a run event, or a message.

## Options

| option | type | default | notes |
|---|---|---|---|
| `root` | string | required | where the place is. An `s3://`/`s3a://` URI, an `az://`/`abfs://`/`abfss://`/`adl://` URI, a `file://` URI, or an ABSOLUTE local path. A relative path is refused with PZDL0101 — pz hands a connector no project-directory anchor, so a relative root would resolve against whatever directory pz was launched from. |
| `region` | string | unset | S3 only. The bucket's region. |
| `access_key_id` | string | unset | S3 only. Give it with `secret_access_key` or with neither; half a pair is refused with PZDL0102 rather than left to fall back to the ambient chain and fail later as a permissions error naming nothing. |
| `secret_access_key` | string | unset | S3 only. The other half of the pair. |
| `session_token` | string | unset | S3 only. For temporary credentials. |
| `endpoint` | string | unset | S3 only. A bare `host[:port]` or a full URL — the two engines disagree about which they want, so the connector strips the scheme for DuckDB and adds one (from `use_ssl`) for delta-rs. Set it for MinIO and other S3-compatible stores. |
| `url_style` | `vhost` or `path` | each engine's own | S3 only. `path` selects path-style addressing, which most S3-compatible stores need. Left unset, neither engine is told anything and each keeps its own default. |
| `use_ssl` | boolean | true | S3 only. `false` allows plain http — for an emulator on localhost, not for anything else. |
| `account_name` | string | unset | Azure only. The storage account. |
| `account_key` | string | unset | Azure only. A shared key for that account. |
| `connection_string` | string | unset | Azure only. A standard Azure Storage connection string. delta-rs has no storage-option key for one, so the connector decomposes it into the keys delta-rs does read (`AccountName`, `AccountKey`, `SharedAccessSignature`, `BlobEndpoint`); DuckDB takes it whole. |
| `tenant_id` | string | unset | Azure only. Part of the service-principal quartet, with `account_name`, `client_id` and `client_secret`. |
| `client_id` | string | unset | Azure only. See `tenant_id`. |
| `client_secret` | string | unset | Azure only. See `tenant_id`. |

An option that belongs to the other family is refused with **PZDL0102** — `region:` under an `az://`
root, `account_name:` under an `s3://` one — rather than silently ignored. Every problem the
connection has is reported at once, not one per run.

## Where an entity's table lives

`root:` says where the *place* is; the entity name says which table in it. A table is
`<root>/<entity>` unless a `path:` option on the read or the write says otherwise, and an absolute
`path:` (or one carrying its own scheme) ignores `root:` entirely.

```yaml
lake:
  connector: deltalake
  root: s3://analytics/curated
  entities:
    orders:            # s3://analytics/curated/orders
      read:
    payments:
      read:
        path: finance/payments   # s3://analytics/curated/finance/payments
```

## How credentials are chosen on Azure

Four mechanisms, tried strictly in this order, one at a time. Both engines are given the *same*
mechanism rather than every configured key at once — otherwise a leftover field from an earlier edit
could have a read and a schema probe of the same table authenticate two different ways.

1. `connection_string`
2. `account_name` + `tenant_id` + `client_id` + `client_secret` (service principal)
3. `account_name` + `account_key`
4. `account_name` alone — managed identity or the ambient Azure credential chain

## How credentials are chosen on S3

`access_key_id` + `secret_access_key` is an explicit key pair. With neither set, DuckDB is given
`provider credential_chain` and delta-rs is left to its own ambient resolution, so an instance
profile or the `AWS_*` environment still works — and any `endpoint`, `region`, `url_style`,
`use_ssl` or `session_token` you configured still reaches both engines. Nothing about the commit
mechanism is left to the environment: see
[how-to/s3-credentials.md](../how-to/s3-credentials.md).

## Secrets are scoped, not just named

Each connection's DuckDB secret carries a `scope` derived from its own `root`. DuckDB does not pick
a secret by name — it matches the path being read against each secret's scope prefixes and, on a
tie, picks alphabetically by secret name. Without a scope, a project with a prod lake and a staging
lake would have whichever name sorts first authenticate every Delta read in the run, and the failure
would surface as a bucket-level 403 naming nothing.

## What is checked, and when

`pz validate` checks this block offline, against a JSON Schema with `additionalProperties: false` —
so a typo'd option is an error, never a silently ignored setting. Connectivity is not probed: opening
the table at run time is the real test, and it fails with a coded message that says more than a
reachability check could.
