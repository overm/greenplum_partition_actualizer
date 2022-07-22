/*
first of all change target schema name. Replace this text:
my.

Ither you can change meta table name. Replace this text:
my.t_table_partition_meta
*/

create or replace function  my.f_table_partition_get_date(varchar, OUT ret date)
language plpgsql
as $main$
begin
    execute 'select '||$1 into ret;
end;
$main$;
-----------


create or replace function  my.f_table_partition_clean_constr(psSchemaTableName varchar, psTableName varchar, psFieldName varchar)
   returns void 
   language plpgsql
as $main$
declare
	vrRow record;
begin
	for vrRow in
    SELECT 'ALTER TABLE '||table_schema||'.'||table_name||' DROP CONSTRAINT '||constraint_name||';' as sqlc
	FROM information_schema.constraint_column_usage
	where 
		table_schema = psSchemaTableName
		and lower(table_name) = psTableName
		and column_name = psFieldName
	loop
		execute vrRow.sqlc;
	end loop;
end;
$main$;
-----------

--If you uncomment this statement, you will lose your data
--drop table if exists my.t_table_partition_meta;
create table if not exists my.t_table_partition_meta
(
	PartTable varchar not null,
	YearDeltaPerMonth int,
	MonthDeltaPerDay int,
	DeltaDays int,
	unique(PartTable)
)with (appendoptimized=false)
distributed by (PartTable)
;
-----------


create or replace function my.f_table_partition_actual(prTable regclass, piYearDeltaPerMonth int, piMonthDeltaPerDay int, piDeltaDays int)
   returns void 
   language plpgsql
  as
$main$
declare 
	viYearDeltaPerMonth int;
	viMonthDeltaPerDay int;
	viDeltaDays int;
	vsTableName varchar;
	vsTableSpaceName varchar;
	vrRow record;
	vsSQL varchar = '';
	
	vdLimit date;
begin
	select c.relname, ns.nspname
		into vsTableName, vsTableSpaceName
	from   pg_catalog.pg_class c
	join pg_catalog.pg_namespace as ns
		on c.relnamespace = ns.oid
	where  c.oid = prTable;

	if vsTableName is null or vsTableSpaceName is null then
		raise exception 'prTable should be set correctly';
	end if;
	
	select YearDeltaPerMonth, MonthDeltaPerDay, DeltaDays
	into viYearDeltaPerMonth, viMonthDeltaPerDay, viDeltaDays
	from my.t_table_partition_meta
	where PartTable = prTable::varchar;
	
	if piYearDeltaPerMonth is null and piMonthDeltaPerDay is null and piDeltaDays is null then
		if viYearDeltaPerMonth is null and viMonthDeltaPerDay is null and viDeltaDays is null then
			raise exception 'Metadata for table %s not found', prTable;
		end if;
	else
		if viYearDeltaPerMonth is null and viMonthDeltaPerDay is null and viDeltaDays is null then			
			insert into my.t_table_partition_meta(PartTable, YearDeltaPerMonth, MonthDeltaPerDay, DeltaDays)
			values(prTable::varchar, piYearDeltaPerMonth, piMonthDeltaPerDay, piDeltaDays);
		else
			update my.t_table_partition_meta
			set YearDeltaPerMonth = piYearDeltaPerMonth,
				MonthDeltaPerDay = piMonthDeltaPerDay,
				DeltaDays = piDeltaDays
			where PartTable = prTable::varchar;
		end if;
		
		viYearDeltaPerMonth = piYearDeltaPerMonth;
		viMonthDeltaPerDay = piMonthDeltaPerDay;
		viDeltaDays = piDeltaDays;
	end if
	;
	
	if viYearDeltaPerMonth is not null then
		vdLimit = date_trunc('month', current_date) + make_interval(months := viYearDeltaPerMonth);
		raise notice 'Year partition actualization till %', vdLimit;
		-- Splitting partitions first of all
		for vrRow in
			with last_part_date as
			(select max(my.f_table_partition_get_date(partitionrangeend)) dt
			from pg_catalog.pg_partitions
			where schemaname = vsTableSpaceName
				and tablename = vsTableName
				and partitiontype = 'range')
			select
				   greatest(generate_series::date, (select dt from  last_part_date)) as start_of_period,
				   least(generate_series::date + make_interval(years := 1), vdLimit)::date as end_of_period
			from generate_series(date_trunc('year', (select dt from  last_part_date)), vdLimit, '1 YEAR')
			where vdLimit > (select dt from  last_part_date)
				and greatest(generate_series::date, (select dt from  last_part_date))
					< least(generate_series::date + make_interval(years := 1),  vdLimit)::date
			order by 1
		loop
			vsSQL = vsSQL || format($$
				alter table %1$s split default partition 
				start (date%2$L) inclusive 
				end (date%3$L) exclusive;
			$$,
				prTable,
				vrRow.start_of_period,
				vrRow.end_of_period);
		end loop;
		raise notice '%', vsSQL;
		execute vsSQL;
		vsSQL = '';
		
		-- Merging partitions
		for vrRow in
			select
				date_trunc('year', my.f_table_partition_get_date(p.partitionrangestart))::date as ownpart,
				my.f_table_partition_get_date(p.partitionrangestart) pt_start,
				my.f_table_partition_get_date(p.partitionrangeend) pt_end,
				f.columnname
			from pg_catalog.pg_partitions p
			inner join pg_catalog.pg_partition_columns f
				on p.schemaname = f.schemaname
				and p.tablename = f.tablename
				and p.partitionlevel = f.partitionlevel
			where p.schemaname = vsTableSpaceName
				and p.tablename = vsTableName
				and p.partitiontype = 'range'
				and (extract(DOY from my.f_table_partition_get_date(p.partitionrangestart)) <> 1)
				and my.f_table_partition_get_date(p.partitionrangeend) <= vdLimit
			order by 2
		loop
			vsSQL = vsSQL || format($$
				drop table if exists %6$s.%7$s;
				create table %6$s.%7$s (like %1$s);
				alter table %1$s exchange partition for (date%2$L )
				with table %6$s.%7$s;

				select my.f_table_partition_clean_constr(%6$L, %7$L, %5$L);

				insert into %6$s.%7$s
				select * from %1$s
				where %5$s >= date%3$L and %5$s < date%4$L;

				alter table %1$s drop partition for (date%2$L);
				alter table %1$s drop partition for (date%3$L);
				alter table %1$s split default partition 
							start (date%2$L) inclusive 
							end (date%4$L) exclusive;
				alter table %1$s exchange partition for (date%2$L)
				with table %6$s.%7$s;
				drop table %6$s.%7$s;
			$$,
				prTable,
				vrRow.ownpart,
				vrRow.pt_start,
				vrRow.pt_end,
				vrRow.columnname,
				vsTableSpaceName,
				vsTableName||'_tmp4prtmdf'
				);
		end loop;
		raise notice '%', vsSQL;
		execute vsSQL;
		vsSQL = '';
	end if;
	
	if viMonthDeltaPerDay is not null then
		vdLimit = current_date + viMonthDeltaPerDay;
		raise notice 'Month partition actualization till %', vdLimit;
		-- Splitting partitions first of all
		for vrRow in
			with last_part_date as
			(select max(my.f_table_partition_get_date(partitionrangeend)) dt
			from pg_catalog.pg_partitions
			where schemaname = vsTableSpaceName
				and tablename = vsTableName
				and partitiontype = 'range')
			select
				   greatest(generate_series::date, (select dt from  last_part_date)) as start_of_period,
				   least(generate_series::date + make_interval(months := 1), vdLimit)::date as end_of_period
			from generate_series(date_trunc('month', (select dt from  last_part_date)), vdLimit, '1 month')
			where vdLimit > (select dt from  last_part_date)
				and greatest(generate_series::date, (select dt from  last_part_date))
					< least(generate_series::date + make_interval(months := 1),  vdLimit)::date
			order by 1
		loop
			vsSQL = vsSQL || format($$
				alter table %1$s split default partition 
				start (date%2$L) inclusive 
				end (date%3$L) exclusive;
			$$,
				prTable,
				vrRow.start_of_period,
				vrRow.end_of_period);
		end loop;
		raise notice '%', vsSQL;
		execute vsSQL;
		vsSQL = '';
		
		-- Merging partitions
		for vrRow in
			select
				date_trunc('month', my.f_table_partition_get_date(p.partitionrangestart))::date as ownpart,
				my.f_table_partition_get_date(p.partitionrangestart) pt_start,
				my.f_table_partition_get_date(p.partitionrangeend) pt_end,
				f.columnname
			from pg_catalog.pg_partitions p
			inner join pg_catalog.pg_partition_columns f
				on p.schemaname = f.schemaname
				and p.tablename = f.tablename
				and p.partitionlevel = f.partitionlevel
			where p.schemaname = vsTableSpaceName
				and p.tablename = vsTableName
				and p.partitiontype = 'range'
				and (extract(DAY from my.f_table_partition_get_date(p.partitionrangestart)) <> 1)
				and my.f_table_partition_get_date(p.partitionrangeend) <= vdLimit
			order by 2
		loop
			vsSQL = vsSQL || format($$
				drop table if exists %6$s.%7$s;
				create table %6$s.%7$s (like %1$s);
				alter table %1$s exchange partition for (date%2$L )
				with table %6$s.%7$s;

				select my.f_table_partition_clean_constr(%6$L, %7$L, %5$L);

				insert into %6$s.%7$s
				select * from %1$s
				where %5$s >= date%3$L and %5$s < date%4$L;

				alter table %1$s drop partition for (date%2$L);
				alter table %1$s drop partition for (date%3$L);
				alter table %1$s split default partition 
							start (date%2$L) inclusive 
							end (date%4$L) exclusive;
				alter table %1$s exchange partition for (date%2$L)
				with table %6$s.%7$s;
				drop table %6$s.%7$s;
			$$,
				prTable,
				vrRow.ownpart,
				vrRow.pt_start,
				vrRow.pt_end,
				vrRow.columnname,
				vsTableSpaceName,
				vsTableName||'_tmp4prtmdf'
				);
		end loop;
		raise notice '%', vsSQL;
		execute vsSQL;
		vsSQL = '';
	end if;
	
	if viDeltaDays is not null then
		vdLimit = current_date + viDeltaDays;
		raise notice 'Daily partition actualization till %', vdLimit;
		-- Splitting partitions first of all
		for vrRow in
			with last_part_date as
			(select max(my.f_table_partition_get_date(partitionrangeend)) dt
			from pg_catalog.pg_partitions
			where schemaname = vsTableSpaceName
				and tablename = vsTableName
				and partitiontype = 'range')
			select
				   greatest(generate_series::date, (select dt from  last_part_date)) as start_of_period,
				   least(generate_series::date + 1, vdLimit)::date as end_of_period
			from generate_series((select dt from  last_part_date), vdLimit, '1 day')
			where vdLimit > (select dt from  last_part_date)
				and greatest(generate_series::date, (select dt from  last_part_date))
					< least(generate_series::date + 1,  vdLimit)::date
			order by 1
		loop
			vsSQL = vsSQL || format($$
				alter table %1$s split default partition 
				start (date%2$L) inclusive 
				end (date%3$L) exclusive;
			$$,
				prTable,
				vrRow.start_of_period,
				vrRow.end_of_period);
		end loop;
		raise notice '%', vsSQL;
		execute vsSQL;
		vsSQL = '';
	end if;
end;
$main$
;

create or replace function my.f_table_partition_actual(prTableName regclass)
   returns void
   language plpgsql
  as
$main$
declare
begin
	perform my.f_table_partition_actual(psTableName, null, null, null);
end;
$main$
;