# phanavision-report

## ETL

Assumes parquet inputs exist in `inputs/`.

Expected files:

- Inputs:
	- `inputs/player_id_map.parquet`
	- `inputs/statcast_era_batted_balls.parquet`
	- `inputs/statcast_2025.parquet`
- Outputs:
	- `output/player_lookup.parquet`
	- `output/hr_report.parquet`
	- `output/hr_report_by_season.parquet`
	- `output/2025_batter_report.parquet`

```bash
uv run phanavision-etl player-lookup --inputs-dir inputs
uv run phanavision-etl hr-report --inputs-dir inputs --output-dir output
uv run phanavision-etl batter-report --inputs-dir inputs --output-dir output
uv run phanavision-etl all --inputs-dir inputs --output-dir output
```

## Run App

```bash
uv run app.py
```

## Production (Linux)

```bash
uv run gunicorn -w 4 -b 0.0.0.0:8050 app:server
```
