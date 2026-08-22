# Incremental ingest from a Delta table

Read only the rows that arrived since the last run, and let DuckDB skip the data files that cannot
contain them.

## The declaration is in the SQL

pz reads an incremental bound from the pipeline's own `WHERE` clause. There is no `incremental:`
block and no `columns:` contract to write — the comparison IS the declaration, and its type comes
from the stored watermark.

```yaml
lake:
  connector: deltalake
  root: /srv/lake
  entities:
    orders:
      read:
```

`warehouse` below is whatever you are loading into — any sink connection:

```sql
INSERT INTO {{ sink('warehouse', 'orders', strategy: 'merge', keys: ['order_id']) }}
select order_id, customer_id, amount, updated_at
from {{ source('lake', 'orders') }}
where updated_at > {{ watermark('lake', 'orders') }}
```

On the first run there is no watermark, so everything lands. Afterwards, pz reads the stored
watermark for `lake.orders`, types the comparison from it, and — after every downstream sink commits
— stores the new one as `MAX(updated_at)` over what landed.

## Why this is worth more on Delta than on a file source

The bound does not just filter rows after reading them. It is pushed into the scan:

```sql
(select * from delta_scan('/srv/lake/orders') where "updated_at" > '2026-08-01T00:00:00')
```

and DuckDB uses the transaction log's per-file `minValues`/`maxValues` for `updated_at` to skip whole
data files without opening them. On a table written in roughly `updated_at` order, that turns a
full-table scan into a few-file scan.

**It only works as well as your file layout.** If `updated_at` is uncorrelated with the order rows
were written in, every file's `[min, max]` covers the whole range, nothing can be skipped, and the
predicate saves you only the rows — not the I/O. That is the same statistics story as
[the primer's §6](../delta-lake-primer.md).

## Pair it with `merge`, not `append`

An incremental read feeding an `append` output is at-least-once, and pz refuses that pairing at
compile time with **PZ0214** unless the output declares `duplicates: accept`. That refusal is doing
its job: the crash window that duplicates an append is real, and an incremental source is exactly
where you meet it. See [../concepts/delivery-guarantees.md](../concepts/delivery-guarantees.md).

With `strategy: merge` and `keys:`, a re-delivered row updates in place instead of duplicating.

## Bounded windows

The bounded-window trio — `initial`, `max_window`, `until` — needs a `columns:` contract, because the
bounds must be computable before the first extraction. When one is in play, the scan is wrapped with
both ends:

```sql
(select * from delta_scan('/srv/lake/orders')
 where "updated_at" > '2026-08-01T00:00:00' and "updated_at" <= '2026-08-02T00:00:00')
```

The connector declares `InclusiveWatermarkBound`, so when the engine hands over an inclusive lower
bound the comparison becomes `>=` rather than `>`.

## What this does not do

- **It does not read the Delta change data feed.** There is no `sync: {mode: cdc}` source here; an
  incremental read is a cursor comparison over the current snapshot, so a row DELETED upstream is
  invisible to it. See [../limitations.md](../limitations.md).
- **It does not time-travel.** The read is always of the table's latest version unless you pin one
  explicitly — see [time-travel.md](time-travel.md).
