psql -c 'drop schema if exists "raw" cascade';
psql -c 'create schema "raw"';
psql -c 'create table "raw"."growth_accounting_users" (
    user_id varchar,
    event_date varchar
);'

gunzip -c $PWD/raw/users.csv | psql -c "copy raw.growth_accounting_users (user_id, event_date)
from stdin
with csv header delimiter ','";