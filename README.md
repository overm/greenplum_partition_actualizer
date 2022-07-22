# greenplum_partition_actualizer
Functions, that help to keep your date partitions up to date
# Installation
First of all take file f_table_partition_actual.sql and change target schema name. Replace this text:<br>
```
my.
```

Either you can change meta table name. Replace this text:

```
t_table_partition_meta
```

Than run it in your database.
# Using
You should have a table with one level range partition on date field.

For example today 2021-07-15:
``` sql
drop TABLE if exists my.agrs;
CREATE TABLE my.agrs (
	agrmnt_id int,
	metric_id smallint,
	start_dt date,
	end_dt date,
	amount DECIMAL) 
DISTRIBUTED BY (agrmnt_id)
PARTITION BY RANGE (end_dt)
  (START (date '2011-01-01')
   END (date '2020-06-01')
   EVERY (INTERVAL '1 YEAR'),
   START (date '2020-06-01')
   END (date '2021-06-19')
   EVERY (INTERVAL '1 MONTH'),
   START (date '2021-06-19')
   END (date '2021-07-19')
   EVERY (INTERVAL '1 MONTH'),
DEFAULT PARTITION hot   );
```

2022-07-22 comes and we launch this command:
``` sql
select * from my.f_table_partition_actual('my.agrs'::regclass, -12,-25,5);
```
where parameters:<br>
1. table that partitions should actual<br>
2. 
