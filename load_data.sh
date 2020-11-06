psql -c 'drop schema if exists "raw" cascade';
psql -c 'create schema "raw"';
psql -c 'create table "raw"."growth_accounting_users" (
    user_id varchar,
    login_at varchar
);'

gunzip -c $PWD/raw/users.csv | psql -c "copy raw.growth_accounting_users (user_id, login_at)
from stdin
with csv header delimiter ','";