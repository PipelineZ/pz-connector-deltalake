# Delivery guarantees

pz publishes a delivery-guarantee matrix as a stability contract:
<https://pipelinez.dev/concepts/delivery-guarantees/>. This page says how Delta maps onto it, and
where the edges are. It does not restate pz's contract — read that one first.

## The short version

| strategy | guarantee | why |
|---|---|---|
| `merge` | effectively-once | idempotent in its keys: running it twice lands the same rows |
| `replace` | effectively-once | total overwrite: running it twice lands the same table |
| `append` | at-least-once | a duplicate is possible in one specific crash window, below |

A Delta commit itself is **atomic**. One commit either becomes a version of the table or does not
exist; there is no half-written state a reader can see. That is why this connector declares the
`Transactional` capability, and it is a genuine claim rather than a hopeful one — the log file either
lands or it does not.

Atomic is not the same as exactly-once, though, and the gap is the whole subject of this page.

## The crash window that can duplicate an append

Execution is staged: the sink buffers, writes data files, then commits the log entry. When that commit
succeeds, the table has the rows. pz then records the SinkWrite as committed in the run's artifacts.

Between those two moments there is a window. If the process dies inside it — power, OOM kill, a
`kill -9` — the table holds the rows and pz does not know it does. `pz retry` will then re-run that
SinkWrite:

- **`append`** writes the rows again. The table now holds them twice. This is the at-least-once
  guarantee, exactly as pz's matrix documents it.
- **`merge`** re-applies the same keys. Rows that were already updated are updated to the same values;
  rows that were already inserted now match and are updated instead of inserted. The table ends up in
  the same state.
- **`replace`** overwrites the table with the same output. Same state.

**This connector does not attempt to close the window for `append`, deliberately.** Closing it
properly needs a stable attempt identity — something that says "this is a retry of that attempt", so
a writer can recognize its own previous commit. Delta has an action for exactly this (`txn`, an
application id plus a version). What is missing is the identity: pz's `OutputSpec` carries the sink,
the output, the strategy, the schema policy, the options, the keys and the delete mode, and nothing
that distinguishes a first run from a retry. A heuristic here would read as a guarantee and not be
one. It is filed as a finding against pz's ABI instead.

## Incremental + append is refused at compile time

pz refuses an incremental source feeding an `append` output with **PZ0214** unless the output
declares `duplicates: accept`. That is pz's rule, not this connector's, and it is the right one: an
incremental read plus an at-least-once write is precisely the combination where the duplicate becomes
visible. Declaring `duplicates: accept` is you saying the downstream tolerates it.

If it does not tolerate it, use `merge` with `keys:`.

## Watermarks advance only after the sinks commit

A new watermark is persisted only after every downstream SinkWrite has committed — including sinks
carried forward from a previous attempt, and those only when their SourceLoads landed as reused. So a
run that extracts rows and then fails to write them does not advance past them, and the next run
re-reads them. Combined with `merge`, that is the shape that gets you effectively-once end to end.

## Four caveats worth knowing before you rely on the table above

**1. `merge` is idempotent in its keys — and only in its keys.** If the table already holds two rows
for one key (left by an older run, by an `append`, or by a writer that is not pz), a merge updates
**every** copy and leaves them all in place. Delta enforces no primary key, and detecting the
duplicate would mean reading the whole target table on every merge. Measured against `DeltaLake.Net`
0.33.0: the write commits, reports success, and the duplicate survives with the new values in both
rows.

**2. `replace` is total, and its window is the whole write session.** It removes the files its own
snapshot listed, taken when the session opened. Rows another writer commits after that — and before
this write commits — are **not** removed, and no conflict is reported. Measured, on local disk:
a table holding ids 0–4, a replace session opens, another writer appends ids 500–504, the replace
commits ids 100–102, and afterwards the table holds 100–102 **and 500–504**. If `replace` must mean
"the table holds exactly these rows", do not run two writers against that output at once.

**3. A lost commit race is reported as transient.** Optimistic concurrency means conflicts are normal:
PZDL0401 is raised with `IsTransient: true` and pz's retry policy decides. Connectors never retry
internally.

**4. On S3, all of this depends on your endpoint.** Every guarantee above assumes the commit is a real
put-if-absent. Against an endpoint that accepts `If-None-Match` and ignores it, two concurrent writers
both "succeed" and one commit is lost — measured, with a two-command probe for checking yours, in
[../limitations.md](../limitations.md).

## What the end-to-end proof does and does not cover

A green `pz run` through the installed connector proves the path works. It does not prove every guard
on the path ran — in particular, pz's `ArrowInterop.NormalizeNativeArrowSchema` forces every field
`nullable: true` before a batch reaches a sink, so an end-to-end run exercises **none** of this
connector's nullability rules. Their only coverage is this repository's own test suite. See
[../compatibility.md](../compatibility.md).
