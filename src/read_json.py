import os
import pandas as pd
from log import setup_logger

logger = setup_logger()

def read_json_file(filepath: str) -> pd.DataFrame:
    """ Read JSON file with error handling """

    try:
        logger.info(f'Reading JSON file path: {filepath}')
        df = pd.read_json(filepath)
        return df

    except ValueError:
        logger.error(f'Invalid JSON format: {filepath}')
        return pd.DataFrame()
    
    except Exception as e:
        logger.exception(f'Error reading JSON file: {e}')
        return pd.DataFrame()