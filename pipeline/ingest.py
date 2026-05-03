import os

from src.data_pipeline import bronze_layer_pipeline_csv, bronze_layer_pipeline_json
from src.utils.config_loader import read_yaml
from src.utils.sessions import get_spark_session

ACCOUNTS_DIR = 'accounts'
TRANSACTIONS_DIR = 'transactions'
CUSTOMERS_DIR = 'customers'

# iso 8601 format
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

CONFIG_PATH = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")


def run_ingestion() -> None:
    """
    Ingest the datasets into a Bronze layer 
    """

    config = read_yaml(CONFIG_PATH)
    bronze_path = config['output']['bronze_path']

    accounts_input_path = config['input']['accounts_path']
    transactions_input_path = config['input']['transactions_path']
    customers_input_path = config['input']['customers_path']

    accounts_output_path = os.path.join(bronze_path, ACCOUNTS_DIR)
    transactions_output_path = os.path.join(bronze_path, TRANSACTIONS_DIR) 
    customers_output_path = os.path.join(bronze_path, CUSTOMERS_DIR)  

    spark_session = get_spark_session(config)

    bronze_layer_pipeline_csv(
        spark_session, 
        accounts_input_path, 
        accounts_output_path, 
        DATETIME_FORMAT
    )
    
    bronze_layer_pipeline_json (
        spark_session,
        transactions_input_path,
        transactions_output_path,
        DATETIME_FORMAT
    )

    bronze_layer_pipeline_csv(
        spark_session, 
        customers_input_path, 
        customers_output_path, 
        DATETIME_FORMAT
    )
