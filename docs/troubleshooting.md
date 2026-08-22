# Troubleshooting

Symptoms this connector's object-store backends produce, and what each one means. Every message below
was reproduced against a real server; none of them is a guess at what a library might say.

## `PZDL0403`: the commit path is not safe against a second writer

```
PZDL0403: the delta append of output 'orders' could not commit through a mechanism that is safe
against a second writer, so nothing was written (...)
```

A commit to `s3://` is a conditional PUT. This code says delta-rs could not use one — so the only
paths left would overwrite another writer's commit, and it refused instead of taking one.

The connector never configures a path like that, so the cause is in the environment delta-rs also
reads:

- **`AWS_S3_ALLOW_UNSAFE_RENAME=true`** — remove it. It does not fix this error; it replaces it with
  commits that disappear silently. There is no configuration in which this connector needs it.
- **`AWS_CONDITIONAL_PUT`** set to anything — remove it. The connector sets `etag` itself.
- **`AWS_S3_LOCKING_PROVIDER=dynamodb`** — remove it. No locking provider is required.

Nothing was written when you see this: the refusal happens before the commit.

## Rows are missing after two runs wrote the same S3 table at once

Both runs reported success and the table holds one run's rows. The endpoint accepted the conditional
PUT's precondition and ignored it, so both writers wrote the same commit file and the later one won.

Check the server, not the pipeline. Amazon S3 enforces `If-None-Match` (since August 2024) and so do
current MinIO builds; older MinIO and some S3-compatible gateways do not. `docs/limitations.md` has
the measurement and the table of what was observed on which build. A single-writer table is
unaffected.

## `az://`: `Generic MicrosoftAzure error: ... HTTP error: builder error`

Every request fails immediately, and the message names neither http nor the endpoint. The cause is a
plain-http `BlobEndpoint` in the connection string: the Azure object-store client refuses one unless
it is told to allow it.

The connector reads the scheme you wrote and opts in for you, so seeing this on a current build means
the endpoint reached delta-rs by another route — check that `connection_string` is the connection
carrying `BlobEndpoint=http://...` rather than an `AZURE_*` environment variable set alongside it.

## `az://`: `Invalid table version: N` on the second write to one table

The first write to a table succeeds; every later one fails with this. Seen against **Azurite** and not
against Azure.

It is an addressing artifact, not a property of the Azure write path. Azurite's `BlobEndpoint` carries
the storage account as a URL **path** segment (`http://host:port/devstoreaccount1`); a real Azure
account carries it in the **host** and has no path, which is the shape the object-store client is
written for. The proof is that the same sequence of commits all succeed against the same Azurite the
moment the client is put into its own emulator mode.

The connector does not switch to emulator mode, deliberately: that mode ignores the configured
endpoint and resolves `127.0.0.1:10000`, which would break every user running Azurite anywhere else.
To develop locally against Delta on Azure, use a real storage account, or accept one commit per table.

## `az://` reads through DuckDB fail against an emulator on a mapped port

`delta_scan` reports a GET against `127.0.0.1:10000` no matter which port the emulator is on. DuckDB's
delta extension reads through delta-kernel-rs, whose object store recognizes an emulator connection
string and resolves the well-known emulator port for it. Run Azurite on port 10000, or read through
delta-rs instead.
