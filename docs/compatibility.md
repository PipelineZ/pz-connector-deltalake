# What is proven, and against what

This connector's backends are not equally covered, and the gaps are stated here rather than left for a
reader to assume away. Every row is a test in this repository; every version is the one the test runs
against.

## Backends

| | local filesystem | S3 (MinIO `RELEASE.2025-09-07T16-13-09Z`) | Azure Blob (Azurite `3.35.0`) |
|---|---|---|---|
| write: `append` | yes | yes | yes, one commit per table |
| write: `replace` | yes | yes | **not proven** |
| write: `merge` | yes | yes | **not proven** |
| read back through delta-rs | yes | yes | yes |
| read back through DuckDB `delta_scan` | yes | yes | yes, only on port 10000 |
| concurrent commits keep every commit | yes | yes | **not proven** |
| credential translation (delta-rs storage options ↔ DuckDB secret) agree | n/a | yes | yes, only on port 10000 |

**`replace` and `merge` on Azure are not proven, and are not claimed.** Azurite accepts one commit per
table when it is driven through a generic endpoint, and both strategies need a seeded target — so
there is no way to exercise them here. Neither is backend-specific: a merge is one MERGE statement
handed to delta-rs, proven on local disk and again on S3. That is a reason to expect them to work, not
evidence that they do.

**Concurrent commits on Azure are not proven** for the same reason.

**ADLS Gen2 (`abfss://`, `abfs://`, `adl://`) is covered by nothing.** Azurite emulates the Blob
endpoint only. Those schemes are accepted by the connector's root classification and unit-tested there
— nothing has ever written a table through one.

**No test in this repository has ever talked to Amazon S3 or to Azure Storage.** Both are emulators.
Where an emulator and the real service differ, this repository measures the emulator; the three places
that difference is known to matter are written up in `docs/limitations.md` and
`docs/troubleshooting.md`:

- **conditional-PUT enforcement on S3** — the endpoint decides whether concurrent commits survive, and
  `limitations.md` carries a two-command probe for checking yours;
- **endpoint addressing on Azure** — Azurite puts the account in the URL path where a real account has
  it in the host;
- **the S3 error body** — real Amazon S3's 403 carries an `<AWSAccessKeyId>` element that MinIO's does
  not, so the redaction covering it is pinned by a hand-fed message
  (`DeltaErrorsTests.Translate_never_leaks_an_access_key_id_out_of_an_s3_xml_error_body`) rather than
  by a container test, which could not reach the shape.

## Versions these facts were measured against

| | version |
|---|---|
| delta-rs | 0.33.0 (`DeltaLake.Net` 0.33.0) |
| DuckDB | 1.5.5 (`DuckDB.NET.Data.Full`) |
| connector ABI | `Pz.Connectors.Abstractions` 0.2.2 |
| measured on | 2026-08-22 |
