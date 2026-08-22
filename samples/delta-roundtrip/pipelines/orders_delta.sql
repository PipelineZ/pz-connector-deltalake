-- The write. `strategy:`, not `mode:` -- pz refuses the retired spelling.
--
-- No `partition_by:` here, and not because the connector lacks it. pz models partition_by as ONE
-- column whose value substitutes calendar tokens ({yyyy}/{MM}/{dd}) in the sink's path:, and refuses
-- the option outright with PZ0219 when the path carries no such tokens. Delta partitions
-- declaratively by column value, with no templated path to route into, so the two meanings do not
-- meet: a Delta table written through pz is unpartitioned, and a merge against it scans the whole
-- table rather than the partitions the write touches. Driven directly, the connector partitions and
-- prunes; through pz today, it cannot be asked to.
INSERT INTO {{ sink('lake', 'orders', strategy: 'merge', keys: ['id']) }}
select id, dt, placed_at, amount
from {{ source('seed', 'orders') }}
