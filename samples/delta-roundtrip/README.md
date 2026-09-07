# delta-roundtrip

A runnable pz project that seeds a Delta table from a CSV and reads it back through DuckDB's
`delta_scan` — the whole external-connector path, declared the way a stranger would declare it:

```yaml
connectors:
  - package: Pz.Connector.DeltaLake
    version: 0.2.0
```

`scripts/verify-external-connector.sh` at the repository root runs this project end to end against a
locally published and packed build. It needs pz 0.5.1 or newer: the connector runs in its own
process, and `docs/installing.md` says what that means for a project.

## Running it

Three things this project needs that a `localfiles`-only project does not:

1. **A version that exists.** `project.yml` pins `0.2.0`, the first release that runs out of
   process. Until that tag exists there is nothing on nuget.org under this id that pz 0.5.1 accepts
   (0.1.0 is an in-process package, refused with PZ0360); the verify script rewrites the line to
   whatever it just packed. Running the sample by hand before that release means pointing
   `pz restore --feeds` at a local folder feed and pinning the version you packed.

2. **An absolute `root:`, supplied through the environment.** This connector declares no
   project-directory anchor, so a relative root would resolve against the working directory of a
   connector process you never launched, and it refuses one (`PZDL0101`). `connections.yml` reads
   `${DELTA_LAKE_ROOT}`:

   ```bash
   export DELTA_LAKE_ROOT="$PWD/out/lake"
   ```

3. **Two runs, not one.** The lake is a store, not a DAG edge. pz derives edges from `ref()`,
   `source()` and `sink()` calls, and nothing connects the pipeline that WRITES `lake.orders` to the
   one that READS it — so they are two independent flows with no ordering between them. Bare `pz run`
   refuses with `PZ0215`, and `pz run --all` would schedule every SourceLoad first, reading the Delta
   table before it exists. Name the flows, in order:

   ```bash
   pz restore --feeds <your-feed> --feeds https://api.nuget.org/v3/index.json
   pz run orders_delta      # data/orders.csv -> a Delta table under $DELTA_LAKE_ROOT
   pz run orders_report     # that Delta table -> out/report/orders_report/*.csv
   ```

`pz restore` downloads 200 MB (the released package ships four platforms) and prints nothing
while it does. It is not hung.

## What it demonstrates, and what it does not

- **`strategy: merge` with `keys:`** — an upsert into a table the first run creates. Re-running
  `orders_delta` updates the seven rows in place rather than duplicating them.
- **A `delta_scan` read** — `orders_report` never pulls Delta rows into .NET; DuckDB scans the table
  directly and aggregates in place.

**What a green run here does NOT prove.** Through pz,
`ArrowInterop.NormalizeNativeArrowSchema` forces every field `nullable: true` before a batch reaches
a sink, so running this sample exercises **none** of the connector's nullability rules — not the
`NOT NULL`-addition refusal, not its `replace` exemption, not the pre-existing-column mirror. Their
only coverage is the connector's own test suite. This sample proves the path works; it does not
prove every guard on the path ran.

It deliberately declares no `partition_by:`. That is not a simplification — pz reads `partition_by`
as a calendar-token path template and refuses it with `PZ0219` for a store that partitions by column
value, so a Delta table written through pz is unpartitioned and a merge against it scans the whole
table. `docs/reference/write.md` measures what that costs.
