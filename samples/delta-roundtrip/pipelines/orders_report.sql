INSERT INTO {{ sink('report', 'orders_report', strategy: 'replace', format: 'csv') }}
select dt, count(*) as orders, sum(amount) as total
from {{ source('lake', 'orders') }}
group by dt
order by dt
