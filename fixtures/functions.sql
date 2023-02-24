create or replace function normalize_drivewealth_symbol(s varchar) returns varchar as
$$
select regexp_replace(regexp_replace($1, '\.([AB])$', '-\1'), '\.(.*)$', '');
$$ language sql;

create or replace function sigmoid(x double precision, beta double precision) returns double precision as
$$
select 1 / (1 + ((x + 1e-10) / (1 - x + 1e-10)) ^ (-beta));
$$ language sql;
