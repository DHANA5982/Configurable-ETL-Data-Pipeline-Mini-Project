import os
import logging
import pandas as pd

class Datapipeline:
    """ class-based data pipeline """
    def __init__(self, data_dir = 'D:/GitHub/Big Data Engineer/ETL Mini Project/data', log_dir = 'D:/GitHub/Big Data Engineer/ETL Mini Project/logs'):
        self.data_dir = data_dir
        self.log_dir = log_dir
        self.setup_logger()

    def setup_logger(self):
        """ Configure Logging """
        os.makedirs(self.log_dir, exist_ok=True)
        logging.basicConfig(
            filename=os.path.join(self.log_dir, 'Pipeline2.log'),
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        logging.info('Logger Initialised.')

    def read_csv(self, filename: str) -> pd.DataFrame:
        """ Read CSV file """
        path = os.path.join(self.data_dir, filename)
        try:
            logging.info(f'Reading CSV file: {path}')
            return pd.read_csv(path)
        except FileNotFoundError:
            logging.error(f'CSV file not found: {path}')
        except Exception as e:
            logging.exception(f'Error reading CSV file: {e}')
        return pd.DataFrame()

    def read_json(self, filename: str) -> pd.DataFrame:
        """ Read JSON file """
        path = os.path.join(self.data_dir, filename)
        try:
            logging.info(f'Reading JSON file: {path}')
            return pd.read_json(path)
        except ValueError as e:
            logging.error(f'Invalid JSON format: {e}')
        except Exception as e:
            logging.exception(f'Error reading JSON file: {e}')
        return pd.DataFrame()
    
    def read_parquet(self, filename: str) -> pd.DataFrame:
        """ Read Parqeut file """
        path = os.path.join(self.data_dir, filename)
        try:
            logging.info(f'Reading Parquet file: {path}')
            return pd.read_parquet(path)
        except FileNotFoundError:
            logging.error(f'Parquet file not found: {path}')
        except Exception as e:
            logging.exception(f'Error reading Parquet file: {e}')
        return pd.DataFrame()
    
    def merge_data(self, sales_df, product_df, region_df):
        """ Merge the three datasets """
        try:
            logging.info('Merging Dataframes...')
            sales_product_df = sales_df.merge(product_df, on = 'product_id', how = 'left')
            full_df = sales_product_df.merge(region_df, on = 'order_id', how = 'left')
            logging.info(f'Merge dataframes sucessful. Dataframe shape: {full_df.shape}')
            return full_df
        except Exception as e:
            logging.exception(f'Error during merge: {e}')
            return pd.DataFrame()
        
    def save_csv(self, df, filename: str):
        """ Save merged data to CSV """
        try:
            path = os.path.join(self.data_dir, filename)
            df.to_csv(path, index=False)
            logging.info(f'Data saved successfully: {path}')
        except Exception as e:
            logging.exception(f'Failed to save file: {e}')

    def run_pipeline(self):
        """ Run end-to-end ETL pipeline """    
        logging.info('----- Pipeline Execution Started -----')
        logging.info('Extracting Data...')
        sales_df = self.read_csv('sales.csv')
        product_df = self.read_json('products.json')
        region_df = self.read_parquet('region.parquet')

        logging.info('Transforming Data...')
        full_df = self.merge_data(sales_df, product_df, region_df)

        if not full_df.empty:
            logging.info('Loading Data...')
            self.save_csv(full_df, 'merged_data_2.csv')
        else:
            logging.warning('Merged dataframe is empty. Nothing to save.')
        
        logging.info('----- Pipeline Execution Completed -----')


if __name__ == "__main__":
    pipeline = Datapipeline()
    pipeline.run_pipeline()