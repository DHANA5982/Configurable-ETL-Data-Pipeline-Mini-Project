import os
import pandas as pd
from log import setup_logger

logger = setup_logger()

def read_parquet_file(filepath: str) -> pd.DataFrame:
    """ Read Parquet file with error handing """

    try:
        logger.info(f'Reading Parquet file: {filepath}')
        df = pd.read_parquet(filepath)
        return df
    
    except FileNotFoundError:
        logger.error(f'Parquet file not found: {filepath}')
        return pd.DataFrame()
    
    except Exception as e:
        logger.exception(f'Error reading Parquet file: {e}')
        return pd.DataFrame()