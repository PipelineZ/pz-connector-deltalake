-- The write. `strategy:`, not `mode:` -- pz refuses the retired spelling.
--
-- `partition_by:` names the columns the TABLE is partitioned by. There is no `path:` here and there
-- is not meant to be: calendar tokens in a path are how pz lays a partitioned output out ITSELF, and
-- a Delta table records its partition columns in its own metadata instead. That is the distinction
-- ConnectorCapabilities.ColumnPartitionedWrites declares, and it is why this connector no longer
-- claims PathTemplating -- it renders no paths in either direction.
--
-- `dt` is a subset of `keys`, which is the only shape in which a merge may derive a partition
-- predicate from the incoming rows: a row's partition value cannot change without its key changing,
-- so the derived predicate cannot hide that row's current partition from the target scan.
INSERT INTO {{ sink('lake', 'orders', strategy: 'merge', keys: ['id', 'dt'], partition_by: ['dt']) }}
select id, dt, placed_at, amount
from {{ source('seed', 'orders') }}
