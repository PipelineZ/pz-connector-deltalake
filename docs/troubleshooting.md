# Troubleshooting

Symptoms this connector's object-store backends produce, and what each one means. Every message below
was reproduced against a real server; none of them is a guess at what a library might say.

## `PZDL0403`: the S3 commit did not go through this connector's commit path

```
PZDL0403: the delta append of output 'orders' was routed through delta-rs's DynamoDB locking log
store, which this connector never configures and cannot supply a lock table for (...)
```

**Cause: `AWS_S3_LOCKING_PROVIDER=dynamodb` in the environment the run inherits.** It is the one
variable measured to break this connector, and it breaks it completely — every S3 write and every S3
*read*, because the table cannot even be opened. It commonly survives in a shell profile or a
container image at a team that used delta-rs's DynamoDB log store before.

**Fix: remove it.** This connector does not need a locking provider. Its commits are conditional PUTs,
which are safe against a second writer with no lock table at all. Nothing else has to be configured in
its place.

**Nothing was written, and there is nothing to clean up.** This failure happens at the table *open* —
before a single data file is produced — so the run never reaches the point of writing anything.
Measured on a refused append to a table whose prefix held three objects: three objects afterwards,
none of them new, and the table still readable with exactly the rows it had.

### The other branch of the same code: a commit path with no concurrency guarantee

`PZDL0403` also covers a commit that could not go through a conditional PUT at all. The message says
so instead, and names `AWS_S3_ALLOW_UNSAFE_RENAME`:

```
PZDL0403: the delta append of output 'orders' could not commit through a mechanism that is safe
against a second writer, so the table was left unchanged (...)
```

No configuration reaches this today — it is a backstop against a future delta-rs whose default moves —
and the only way to provoke it is to take the conditional PUT away deliberately.

**Your table is unchanged, but one orphan file may be left behind.** Unlike the DynamoDB cause above,
this one fails at the *log entry*, which is the last thing a write does: delta-rs writes the data
files first and commits the log entry last. Measured on the same three-object table: **four** objects
afterwards, the new one a `part-*.snappy.parquet`, and the table still readable with exactly the rows
it had. No reader will ever see that file — it is in no commit — but it is storage you are paying for
until you remove it.

### Two variables that do NOT cause this, and cannot

Measured against delta-rs 0.33.0 on 2026-08-22, exported into the process environment:

| exported | effect |
|---|---|
| `AWS_S3_ALLOW_UNSAFE_RENAME=true` | **none.** The write still commits through the conditional PUT. |
| `AWS_CONDITIONAL_PUT=disabled` | **none.** The write still succeeds. |

The connector sets `AWS_CONDITIONAL_PUT` explicitly as a storage option, and a storage option beats the
environment — so a shell cannot force this connector onto an unsafe commit path. Removing either
variable will not fix a failing run, because neither can have caused one.

## Rows are missing after two runs wrote the same S3 table at once

Both runs reported success and the table holds one run's rows. The endpoint accepted the conditional
PUT's precondition and ignored it, so both writers wrote the same commit file and the later one won.

Check the server, not the pipeline. `docs/limitations.md` has a two-command probe that answers it
outright — two conditional PUTs of the same key, where a second success means your endpoint will lose
commits — plus the measured table of what was observed on which MinIO build. Current MinIO builds
enforce the precondition and older ones do not; AWS documents enforcement since August 2024, which
this repository has not verified. A single-writer table is unaffected.

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
