create or replace function normalize_drivewealth_symbol(s varchar) returns varchar as
$$
select regexp_replace(regexp_replace($1, '\.([AB])$', '-\1'), '\.(.*)$', '');
$$ language sql;
