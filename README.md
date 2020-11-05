# dbt-growth-accounting

DBT Package for calculating "growth accounting" metrics.

## Development

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