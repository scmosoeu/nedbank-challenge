"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  → dim_accounts.account_sk
      fact_transactions.customer_sk → dim_customers.customer_sk
      dim_accounts.customer_id      → dim_customers.customer_id
  - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.
  - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.

See output_schema_spec.md for the complete field-by-field specification.
"""

import os
from src.schemas.accounts_schema import update_accounts_schema_gold
from src.schemas.customers_schema import update_customers_schema_gold
from src.schemas.transactions_schema import update_transactions_schema_gold
from src.utils.config_loader import read_yaml
from src.utils.file_reader import read_delta_table
from src.utils.file_writer import write_delta_table
from src.utils.sessions import get_spark_session


ACCOUNTS_DIR = 'accounts'
TRANSACTIONS_DIR = 'transactions'
CUSTOMERS_DIR = 'customers'

CONFIG_PATH = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")

def run_provisioning():
    # TODO: Implement Gold layer provisioning.
    #
    # Suggested steps:
    #   1. Load pipeline_config.yaml to get input/output paths.
    #   2. Initialise (or reuse) SparkSession.
    #   3. Read Silver tables.
    #   4. Build dim_customers with surrogate keys and derived age_band.
    #   5. Build dim_accounts with surrogate keys; rename customer_ref → customer_id.
    #   6. Build fact_transactions, resolving account_sk and customer_sk via joins.
    #   7. Write all three Gold tables as Delta Parquet.
    #   8. (Stage 2+) Write dq_report.json to /data/output/.
    
    config = read_yaml(CONFIG_PATH)
    silver_path = config['output']['silver_path']
    gold_path = config['output']['gold_path']

    accounts_input_path = os.path.join(silver_path, ACCOUNTS_DIR)
    transactions_input_path = os.path.join(silver_path, TRANSACTIONS_DIR) 
    customers_input_path = os.path.join(silver_path, CUSTOMERS_DIR)

    accounts_output_path = os.path.join(gold_path, f"dim_{ACCOUNTS_DIR}")
    transactions_output_path = os.path.join(gold_path, f"fact_{TRANSACTIONS_DIR}") 
    customers_output_path = os.path.join(gold_path, f"dim_{CUSTOMERS_DIR}")

    spark_session = get_spark_session(config)

    accounts_df = read_delta_table(spark_session, accounts_input_path)
    dim_accounts_df = update_accounts_schema_gold(accounts_df)
    write_delta_table(dim_accounts_df, accounts_output_path)


    customers_df = read_delta_table(spark_session, customers_input_path)
    dim_customers_df = update_customers_schema_gold(customers_df)
    write_delta_table(dim_customers_df, customers_output_path)

    # Create a mapping between customer_id and account_id to get
    # account_sk and customer_sk into fact_transactions
    customer_account_df = (
        dim_customers_df.select('customer_id', 'customers_sk')
        .join(
            other=dim_accounts_df.select(
                'account_id', 'customer_id', 'accounts_sk'
            ),
            on='customer_id',
            how='inner'
        ).drop('customer_id')
    )

    transactions_df = read_delta_table(spark_session, transactions_input_path)
    fact_transactions_df = update_transactions_schema_gold(transactions_df)

    fact_transactions_df = fact_transactions_df.join(
        other=customer_account_df,
        on='account_id',
        how='left'
    ).drop('account_id')

    write_delta_table(fact_transactions_df, transactions_output_path)
