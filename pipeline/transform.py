"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Input paths (Bronze layer output — read these, do not modify):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Output paths (your pipeline must create these directories):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Requirements:
  - Deduplicate records within each table on natural keys
    (account_id, transaction_id, customer_id respectively).
  - Standardise data types (e.g. parse date strings to DATE, cast amounts to
    DECIMAL(18,2), normalise currency variants to "ZAR").
  - Apply DQ flagging to transactions:
      - Set dq_flag = NULL for clean records.
      - Set dq_flag to the appropriate issue code for flagged records.
      - Valid codes: ORPHANED_ACCOUNT, DUPLICATE_DEDUPED, TYPE_MISMATCH,
        DATE_FORMAT, CURRENCY_VARIANT, NULL_REQUIRED.
  - At Stage 2, load DQ rules from config/dq_rules.yaml rather than hardcoding.
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.

See output_schema_spec.md §8 for the full list of DQ flag values and their
definitions.
"""

import os
from src.schemas.accounts_schema import update_accounts_schema_silver
from src.schemas.customers_schema import update_customers_schema_silver
from src.schemas.transactions_schema import update_transactions_schema_silver
from src.utils.config_loader import read_yaml
from src.utils.dataframe_transforms import remove_duplicates
from src.utils.file_reader import read_delta_table
from src.utils.file_writer import write_delta_table
from src.utils.sessions import get_spark_session

ACCOUNTS_DIR = 'accounts'
TRANSACTIONS_DIR = 'transactions'
CUSTOMERS_DIR = 'customers'

CONFIG_PATH = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")


def run_transformation() -> None:
    """
    Extract the datasets from the Bronze layer and 
    ingest them into a Silver layer after applying
    transformations
    """
    # TODO: Implement Silver layer transformation.
    #
    # Suggested steps:
    #   1. Load pipeline_config.yaml to get input/output paths.
    #   2. Initialise (or reuse) SparkSession.
    #   3. Read each Bronze table.
    #   4. Deduplicate, type-cast, and standardise each table.
    #   5. Apply DQ flagging to the transactions table.
    #   6. Write cleaned tables to silver/.
    
    config = read_yaml(CONFIG_PATH)
    bronze_path = config['output']['bronze_path']
    silver_path = config['output']['silver_path']

    accounts_input_path = os.path.join(bronze_path, ACCOUNTS_DIR)
    transactions_input_path = os.path.join(bronze_path, TRANSACTIONS_DIR) 
    customers_input_path = os.path.join(bronze_path, CUSTOMERS_DIR)

    accounts_output_path = os.path.join(silver_path, ACCOUNTS_DIR)
    transactions_output_path = os.path.join(silver_path, TRANSACTIONS_DIR) 
    customers_output_path = os.path.join(silver_path, CUSTOMERS_DIR)  

    spark_session = get_spark_session(config)

    accounts_df = read_delta_table(spark_session, accounts_input_path)
    accounts_df = remove_duplicates(accounts_df, subset=['account_id'])
    accounts_df = update_accounts_schema_silver(accounts_df)
    write_delta_table(accounts_df, accounts_output_path)

    customers_df = read_delta_table(spark_session, customers_input_path)
    customers_df = remove_duplicates(customers_df, subset=['customer_id'])
    customers_df = update_customers_schema_silver(customers_df)
    write_delta_table(customers_df, customers_output_path)

    transactions_df = read_delta_table(spark_session, transactions_input_path)
    transactions_df = remove_duplicates(transactions_df, subset=['transaction_id'])
    transactions_df = update_transactions_schema_silver(transactions_df)
    write_delta_table(transactions_df, transactions_output_path)