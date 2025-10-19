import os
import pandas as pd
from log import setup_logger

logger = setup_logger()

def read_csv_file(filepath: str) -> pd.DataFrame:
    """ Read a CSV file with error handling """

    try:
        logger.info(f'Reading CSV file path: {filepath}')
        df = pd.read_csv(filepath)
        return df

    except FileNotFoundError:
        logger.error(f'CSV file not found: {filepath}')
        return pd.DataFrame()
    
    except Exception as e:
        logger.exception(f'Error reading CSV file: {e}')
        return pd.DataFrame()