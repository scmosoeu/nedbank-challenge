"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Input paths (read-only mounts — do not write here):
  /data/input/accounts.csv
  /data/input/transactions.jsonl
  /data/input/customers.csv

Output paths (your pipeline must create these directories):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Requirements:
  - Preserve source data as-is; do not transform at this layer.
  - Add an `ingestion_timestamp` column (TIMESTAMP) recording when each
    record entered the Bronze layer. Use a consistent timestamp for the
    entire ingestion run (not per-row).
  - Write each table as a Delta Parquet table (not plain Parquet).
  - Read paths from config/pipeline_config.yaml — do not hardcode paths.
  - All paths are absolute inside the container (e.g. /data/input/accounts.csv).

Spark configuration tip:
  Run Spark in local[2] mode to stay within the 2-vCPU resource constraint.
  Configure Delta Lake using the builder pattern shown in the base image docs.
"""

import os
from delta.tables import DeltaTable
from datetime import datetime

from src.utils import read_yaml, add_ingestion_timestamp, get_datetime_now
from src.sessions import get_spark_session


CONFIG_PATH = os.environ.get("PIPELINE_CONFIG", "data/config/pipeline_config.yaml")
DATETIME_FORMAT = '%Y-%m-%d'

def write_delta_table(df, path):
    df.write.format("delta").mode("append").save(path)



def run_ingestion():
    # TODO: Implement Bronze layer ingestion.
    #
    # Suggested steps:
    #   1. Load pipeline_config.yaml to get input/output paths.
    #   2. Initialise a SparkSession with Delta Lake support (local[2]).
    #   3. Read accounts.csv → append ingestion_timestamp → write to bronze/accounts/.
    #   4. Read transactions.jsonl → append ingestion_timestamp → write to bronze/transactions/.
    #   5. Read customers.csv → append ingestion_timestamp → write to bronze/customers/.

    
    config = read_yaml(CONFIG_PATH)
    session = get_spark_session(config)

    ingestion_timestamp = get_datetime_now(DATETIME_FORMAT)

    df = add_ingestion_timestamp(df, ingestion_timestamp)
