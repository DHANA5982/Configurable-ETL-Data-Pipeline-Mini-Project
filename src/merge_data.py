import pandas as pd
from log import setup_logger

logger = setup_logger()

def merge_data(sales_df, product_df, region_df):
    """ Merge the three datasets"""

    try:
        logger.info('Merging dataframes')
        sale_product_df = sales_df.merge(product_df, on = 'product_id', how = 'left')
        full_df = sale_product_df.merge(region_df, on = 'order_id', how = 'left')
        logger.info(f'Merged Dataframe Shape: {full_df.shape}')
        return full_df
    
    except Exception as e:
        logger.exception(f'Error merging dataframes: {e}')
        return pd.DataFrame