# Databricks Price Pipeline Lab

A small hands-on migration of a historical equity-price pipeline to Databricks Serverless.

The project ingests CSV price files from a Unity Catalog Volume and produces clean daily price data and timing features using PySpark and Delta Lake.

## Architecture

```text
CSV files in Unity Catalog Volume
        ↓
Bronze Delta: raw prices + ingestion metadata
        ↓
Silver Delta: typed, validated, deduplicated daily prices
        ↓
Gold Delta: returns, momentum and volume features
```

## What this project demonstrates

* Unity Catalog schema and Volume usage
* PySpark transformations on Databricks Serverless
* Bronze / Silver / Gold data design
* Delta Lake writes and `MERGE` upserts
* File-once Bronze ingestion using source-file metadata
* Data-quality checks for unique price keys and valid OHLC values
* Window functions for rolling returns, highs and volume averages
* A Databricks Workflow to run the pipeline
* Git-backed Databricks notebook development with GitHub

## Data flow

### Bronze — `workspace.price_lab.bronze_price_raw`

Reads CSV files from:

```text
/Volumes/workspace/price_lab/landing/prices
```

Bronze preserves the original values and adds:

* `_ingested_at`
* `_source_file`

Previously ingested source files are skipped on later runs, preventing duplicate Bronze ingestion.

### Silver — `workspace.price_lab.silver_price_daily`

Silver standardises the raw data:

* normalises ticker symbols;
* parses multiple date formats;
* casts prices and volume to appropriate types;
* filters invalid records;
* keeps one row per `ticker` and `price_date`;
* uses a Delta `MERGE` to update existing keys and insert new ones.

### Gold — `workspace.price_lab.gold_price_timing_features`

Gold creates features for downstream analysis, including:

* 1-day, 5-day and 10-day returns;
* momentum acceleration;
* 20-day high and distance from that high;
* 5-day and 10-day average volume;
* volume-spike ratios.

## Data-quality checks

The pipeline fails early if Silver contains:

* duplicate `ticker + price_date` keys;
* `low > high`;
* a close price outside the daily low/high range;
* negative volume.

## Running the pipeline

The notebook has job parameters:

| Parameter     | Default                                       |
| ------------- | --------------------------------------------- |
| `catalog`     | `workspace`                                   |
| `schema`      | `price_lab`                                   |
| `source_path` | `/Volumes/workspace/price_lab/landing/prices` |
| `run_mode`    | `full`                                        |

A Databricks Workflow named `price_pipeline_daily` runs the Git-backed notebook on Serverless compute.

## Repository structure

```text
src/
  price_pipeline_lab.py
```

## Notes

This is a learning project built with public/sample historical-price data. No credentials, production data or proprietary code are included.
