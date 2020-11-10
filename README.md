# dbt-growth-accounting

DBT Package for calculating "growth accounting" metrics.

Documentation for each of the models is available on [a DBT docs site](https://tjwaterman99.github.io/dbt-growth-accounting).

## Development

Start a Postgres database.

```
docker run \
    --publish 5432:5432 \
     -e POSTGRES_PASSWORD=dev \
     -e POSTGRES_USER=dev \
     -e POSTGRES_DATABASE=dev \
     --name postgres \
     --restart unless-stopped \
     --detach \
     postgres:12
```

Install the project requirements.

```
pip install -r requirements.txt
```

Load the sample data into a local postgres database.

```
./load_data.sh
```

Build the dbt models.

```
dbt run --profiles-dir .dbt
```

Test the dbt models.

```
dbt test --profiles-dir .dbt
```

Build the dbt docs.

```
dbt docs generate --profiles-dir .dbt
dbt docs serve --profiles-dir .dbt --no-browser
```