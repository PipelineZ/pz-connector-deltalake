# Upsert into a Delta table with `keys:`

`strategy: merge` updates the rows already in the table and inserts the ones that are not. `keys:`
names the columns that identify a row, and it is required — a merge without it is **PZDL0103**.

```sql
INSERT INTO {{ sink('lake', 'orders', strategy: 'merge', keys: ['order_id']) }}
select order_id, customer_id, amount, updated_at
from {{ ref('stg_orders') }}
```

or, on the other surface:

```yaml
lake:
  connector: deltalake
  root: /srv/lake
  entities:
    orders:
      write:
        strategy: merge
        keys: [order_id]
```

Behind it, the connector generates a full `MERGE` statement — delta-rs takes a statement, not a
predicate — joining the target and the incoming batch on those columns, updating every non-key column
of a matched row, and inserting every row that does not match. Every identifier is quoted; nothing you
supply is interpolated unquoted.

## The table is created on first run

There is nothing to create beforehand. The first merge into a location with no `_delta_log` creates
the table with the schema the pipeline produces.

## Rules the keys have to satisfy

| rule | what happens if it does not hold |
|---|---|
| every key names a column the pipeline actually selects | PZDL0303 |
| keys are scalar — number, string, boolean, date, timestamp | a list, struct or map key is PZDL0305 |
| no key value is NULL | PZDL0405 |
| no float or double key value is NaN | PZDL0405 |
| no column name contains a `"` | PZDL0304 |

The null and NaN rules are the two that surprise people. `target.k = source.k` is *never true* against
a null, and NaN never equals itself — so such a row matches nothing, and the merge would insert
another copy of it on every single run while reporting success. Refusing the write is the only outcome
that is not silently wrong. (`-0.0` is fine and is not refused: `-0.0 = 0.0` is true.)

Those checks read the batches the write hands over. A null or NaN key **already sitting in the table**
that no incoming row touches is outside their reach — reading the whole target on every merge would
cost more than the check protects.

## The same key twice in one write

A pipeline's `SELECT` is not deduplicated, so a write can legitimately carry the same key more than
once. **The last row for that key is the one that lands**, and the table ends up with exactly one row
per key.

That resolution is this connector's, not Delta's, and it has to be. A raw Delta `MERGE` matches the
*target* against the source, so two source rows for one key are two independent matches: if the key is
already in the table delta-rs refuses the whole statement, and if it is not, both rows fall to
`WHEN NOT MATCHED` and **both are inserted** — one commit, no error, and a duplicate of a key the
output declared unique.

## What a merge cannot fix

**A duplicate already in the table is not detected, and not repaired.** Delta enforces no primary key,
so a table can already hold two rows for one key — left by an earlier `append`, or by a writer that is
not pz. A merge updates **every** copy and leaves them all in place. Measured against `DeltaLake.Net`
0.33.0: the write commits, reports success, and the duplicate survives with the new values in both
rows. If you suspect duplicates, check the table directly; nothing here will tell you.

## Merges get slower as the table grows

Merge cost follows the files the merge touches, not the rows it changes — a one-row update rewrites
the whole Parquet file that row lives in. On a large table without a way to narrow the target scan,
that is the difference between 85 ms and 1.6 s, and it widens.

Read [tune-a-slow-merge.md](tune-a-slow-merge.md) before merging into anything large. Read it before
reaching for `merge_predicate:` in particular: a row that predicate excludes is **duplicated**, not
skipped.

## Deletes

There is no delete. `on_delete: delete|soft` fails with **PZ0339**, because the `ApplyDeletes`
capability is deliberately not declared: the change-capture *source* half does not exist yet, and a
half-wired capability is worse than an absent one.
