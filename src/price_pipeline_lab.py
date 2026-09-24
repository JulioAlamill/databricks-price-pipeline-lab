# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Price pipeline — Databricks learning lab
# MAGIC
# MAGIC This is a deliberately small migration of your existing price pipeline:
# MAGIC
# MAGIC `historical prices file -> Bronze Delta -> Silver Delta -> Gold timing features`
# MAGIC
# MAGIC It keeps the existing pipeline as the functional benchmark.  This lab teaches
# MAGIC the Databricks patterns first: Volumes, Delta tables, Spark transformations,
# MAGIC idempotent loads, data-quality checks and a Job-ready entry point.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable

# Widgets are Job parameters when the notebook is run by a Databricks Workflow.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "price_lab")
dbutils.widgets.text("source_path", "/Volumes/workspace/price_lab/landing/prices")
dbutils.widgets.dropdown("run_mode", "full", ["full", "transform_only"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_path = dbutils.widgets.get("source_path")
run_mode = dbutils.widgets.get("run_mode")

bronze_table = f"{catalog}.{schema}.bronze_price_raw"
silver_table = f"{catalog}.{schema}.silver_price_daily"
gold_table = f"{catalog}.{schema}.gold_price_timing_features"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze: preserve the source data
# MAGIC
# MAGIC Upload an existing historical-price CSV export to the `landing/prices`
# MAGIC Volume. It needs these FMP-style columns: `ticker`, `date`, `open`, `high`,
# MAGIC `low`, `close`, `adjClose`, `volume`.
# MAGIC
# MAGIC Bronze is append-only in principle: it keeps the supplied values and adds
# MAGIC ingestion metadata. No business cleaning belongs here.

# COMMAND ----------

# DBTITLE 1,Bronze: ingest only new files
raw_prices = (
    spark.read.option("header", True)
    .option("recursiveFileLookup", "true")
    .csv(source_path)
)

bronze_with_metadata = raw_prices.withColumn(
    "_ingested_at", F.current_timestamp()
).withColumn(
    "_source_file", F.col("_metadata.file_path")
)

if spark.catalog.tableExists(bronze_table):
    already_processed = spark.table(bronze_table).select("_source_file").distinct()
    new_bronze = bronze_with_metadata.join(
        already_processed,
        on="_source_file",
        how="left_anti"
    )
else:
    new_bronze = bronze_with_metadata

new_count = new_bronze.count()

if new_count == 0:
    print("No new files to process — skipping Bronze append.")
else:
    new_bronze.write.format("delta").mode("append").saveAsTable(bronze_table)
    print(f"Bronze rows appended: {new_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver: typed, clean and deduplicated daily prices
# MAGIC
# MAGIC The MERGE makes this step safe to rerun. The natural business key is
# MAGIC `(ticker, price_date)`: a later rerun updates that price instead of creating
# MAGIC a duplicate.

# COMMAND ----------

bronze = spark.table(bronze_table)

silver_source = (
    bronze.select(
        F.upper(F.trim("ticker")).alias("ticker"),
     F.coalesce(
            F.expr("try_to_date(`date`, 'yyyy-MM-dd HH:mm:ss')"),
            F.expr("try_to_date(`date`, 'yyyy-MM-dd')"),
            F.expr("try_to_date(`date`, 'dd/MM/yyyy HH:mm')"),
            F.expr("try_to_date(`date`, 'dd/MM/yyyy')"),
            ).alias("price_date"),
        F.col("open").cast("double").alias("open"),
        F.col("high").cast("double").alias("high"),
        F.col("low").cast("double").alias("low"),
        F.col("close").cast("double").alias("close"),
        F.col("adjClose").cast("double").alias("adj_close"),
        F.col("volume").cast("long").alias("volume"),
        F.col("_ingested_at"),
    )
    .where(F.col("ticker").isNotNull() & (F.length("ticker") > 0))
    .where(F.col("price_date").isNotNull())
    .where(F.col("close").isNotNull() & (F.col("close") > 0))
)

# Keep the latest ingested version if the landing file contains the same ticker/date twice.
dedupe_window = Window.partitionBy("ticker", "price_date").orderBy(F.col("_ingested_at").desc())
silver_source = (
    silver_source.withColumn("_row_number", F.row_number().over(dedupe_window))
    .where(F.col("_row_number") == 1)
    .drop("_row_number")
)

if not spark.catalog.tableExists(silver_table):
    silver_source.write.format("delta").mode("overwrite").saveAsTable(silver_table)
else:
    target = DeltaTable.forName(spark, silver_table)
    (
        target.alias("target")
        .merge(silver_source.alias("source"), "target.ticker = source.ticker AND target.price_date = source.price_date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data-quality checks
# MAGIC
# MAGIC Fail the run early for the two checks that matter most at this stage:
# MAGIC unique price keys and valid OHLC values. In a later iteration, record these
# MAGIC checks in a monitoring table rather than only failing the Job.

# COMMAND ----------

silver = spark.table(silver_table)
duplicate_key_count = (
    silver.groupBy("ticker", "price_date").count().where("count > 1").count()
)
invalid_ohlc_count = silver.where(
    (F.col("low") > F.col("high"))
    | (F.col("close") < F.col("low"))
    | (F.col("close") > F.col("high"))
    | (F.col("volume") < 0)
).count()

assert duplicate_key_count == 0, f"Silver has {duplicate_key_count} duplicate price keys"
assert invalid_ohlc_count == 0, f"Silver has {invalid_ohlc_count} invalid OHLC rows"
print(f"Silver quality checks passed. Rows: {silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold: a small feature table
# MAGIC
# MAGIC This is the Spark equivalent of the early part of your pandas timing-feature
# MAGIC logic. It is intentionally *not* a full port of RSI/SMI/MACD and your R1
# MAGIC scoring yet. First validate these outputs beside your current pipeline.

# COMMAND ----------

history = Window.partitionBy("ticker").orderBy("price_date")
last_20_days = history.rowsBetween(-19, 0)
last_5_days = history.rowsBetween(-4, 0)
last_10_days = history.rowsBetween(-9, 0)

gold = (
    silver
    .withColumn("previous_adj_close", F.lag("adj_close", 1).over(history))
    .withColumn("adj_close_5d_ago", F.lag("adj_close", 5).over(history))
    .withColumn("adj_close_10d_ago", F.lag("adj_close", 10).over(history))
    .withColumn("ret_1d", F.try_divide(F.col("adj_close"), F.col("previous_adj_close")) - 1)
    .withColumn("ret_5d", F.try_divide(F.col("adj_close"), F.col("adj_close_5d_ago")) - 1)
    .withColumn("ret_10d", F.try_divide(F.col("adj_close"), F.col("adj_close_10d_ago")) - 1)
    .withColumn("momentum_acceleration", F.col("ret_5d") - F.col("ret_10d"))
    .withColumn("high_20d", F.max("adj_close").over(last_20_days))
    .withColumn("distance_to_high_20d", F.try_divide(F.col("adj_close"), F.col("high_20d")) - 1)
    .withColumn("volume_avg_5d", F.avg("volume").over(last_5_days))
    .withColumn("volume_avg_10d", F.avg("volume").over(last_10_days))
    .withColumn("volume_spike_5d", F.try_divide(F.col("volume"), F.col("volume_avg_5d")))
    .withColumn("volume_spike_10d", F.try_divide(F.col("volume"), F.col("volume_avg_10d")))
    .drop("previous_adj_close", "adj_close_5d_ago", "adj_close_10d_ago")
)

gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_table)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Query the latest features

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT ticker, price_date, adj_close, ret_1d, ret_5d,
               distance_to_high_20d, volume_spike_5d
        FROM {gold_table}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY price_date DESC) = 1
        ORDER BY ret_5d DESC NULLS LAST
        LIMIT 30
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### First Databricks Workflow
# MAGIC
# MAGIC Create one Workflow task that runs this notebook daily after market data has
# MAGIC been placed in the landing Volume. Pass `catalog`, `schema`, `source_path`
# MAGIC and `run_mode` as task parameters. The next migration step is to replace the
# MAGIC manual file drop with FMP/API ingestion and only then port the rest of the TA
# MAGIC calculations after checking parity with the existing Python/PostgreSQL output.