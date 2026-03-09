# 532-dbdemo

Demo apps for DSCI 532 Lecture 9 — Databases in Shiny.

Each `app-XX.py` is a standalone Shiny for Python app. Open the repo in Posit Cloud and select whichever app you want to run as the main file.

| App | What it shows |
|---|---|
| `app-01-ibis-intro.py` | ibis + DuckDB with palmerpenguins |
| `app-02a-taxi-inmem.py` | NYC taxi — in-memory DuckDB (small sample) |
| `app-02b-taxi-parquet.py` | NYC taxi — lazy parquet loading (`python prep_data.py` first) |
| `app-03-form.py` | Simple feedback form (in-memory only) |
| `app-03a-log-local.py` | Form logging to local CSV file |
| `app-03b-log-mongodb.py` | Form logging to MongoDB Atlas |
| `app-04a-log-local.py` | querychat logger — local CSV backend |
| `app-04b-log-mongodb.py` | querychat logger — MongoDB Atlas backend |

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your keys
```

For `app-02b`, download the parquet data first:
```bash
python prep_data.py
```
