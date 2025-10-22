import os
import yaml
import logging
import argparse
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()
class Datapipeline:
    def __init__(self, config_path = '../config/config.yaml'):
        self.config = self.load_config(config_path)
        self.log_dir = self.config['logging']['log_dir']
        self.setup_logger()
        self.data_paths = self.config['data_paths']
        self.db_config = self.config['database']
        # Override YAML values with .env if they exist
        self.db_config["user"] = os.getenv("POSTGRES_USER", self.db_config.get("user"))
        self.db_config["password"] = os.getenv("POSTGRES_PASSWORD", self.db_config.get("password"))
        self.db_config["host"] = os.getenv("POSTGRES_HOST", self.db_config.get("host"))
        self.db_config["port"] = os.getenv("POSTGRES_PORT", self.db_config.get("port"))
        self.db_config["name"] = os.getenv("POSTGRES_DB", self.db_config.get("name"))

    # ---------------- Configuration ---------------- #
    def load_config(self, path: str):
        try:
            with open(path, 'r') as file:
                config = yaml.safe_load(file)
                return config
        except FileNotFoundError:
            raise FileNotFoundError(f'Config file not found: {path}')
        except yaml.YAMLError as e:
            raise ValueError(f'Error parsing YAML config: {e}')
        
    # ---------------- Logging ---------------- #
    def setup_logger(self):
        os.makedirs(self.log_dir, exist_ok=True)
        log_file = os.path.join(self.log_dir, self.config['logging']['log_file'])
        logging.basicConfig(
            filename=log_file,
            level=getattr(logging, self.config['logging']['log_level']),
            format='%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        logging.info('Logger initailised from config.')

    # ---------------- File Readers ---------------- #
    def read_file(self, path: str, file_type: str) -> pd.DataFrame:
        try:
            logging.info(f'Reading {file_type.upper()} file: {path}')
            if file_type == 'csv':
                return pd.read_csv(path)
            elif file_type == 'json':
                return pd.read_json(path)
            elif file_type == 'parquet':
                return pd.read_parquet(path)
            else:
                logging.error(f'Unsupported file type: {file_type}')
                return pd.DataFrame()
        except Exception as e:
            logging.exception(f'Error reading {file_type} file: {e}')
            return pd.DataFrame()
    
    # ---------------- Transform ---------------- #
    def merge_data(self, sales_df, products_df, region_df):
        try:
            logging.info('Merging datasets...')
            sale_product = sales_df.merge(products_df, on = 'product_id', how = 'left')
            full_df = sale_product.merge(region_df, on = 'order_id', how = 'left')
            logging.info(f'Merge complete. Dataframe shape: {full_df.shape}')
            return full_df
        except Exception as e:
            logging.info(f'Merge failed: {e}')
            return pd.DataFrame()
    
    # ---------------- Save to file ---------------- #
    def save_output(self, df):
        try:
            output_path = self.data_paths['output_file']
            fmt = self.config['pipeline']['output_format']
            if fmt == 'csv':
                df.to_csv(output_path, index=False)
            elif fmt == 'parquet':
                df.to_parquet(output_path, index=False)
            else:
                logging.error(f'Unsupported output format: {fmt}')
                return
            logging.info(f'Data saved: {output_path}')
        except Exception as e:
            logging.exception(f'Failed to save output: {e}')

    # ---------------- Database Load ---------------- #
    def load_to_db(self, df):
        db = self.db_config
        try:
            if db['type'] == 'postgres':
                connection_string = (
                    f"postgresql+psycopg2://{db['user']}:{db['password']}"
                    f"@{db['host']}:{db['port']}/{db['name']}"
                )
            elif db['type'] == 'sqlite':
                connection_string = f"sqlite:///{db['name']}"
            else:
                logging.error(f'Unsupported database type: {db['type']}')
                return
            
            logging.info(f'Connecting to DB: {connection_string}')
            engine = create_engine(connection_string)

            df.to_sql(db['table'], engine, if_exists='replace', index=False)
            logging.info(f'Data successfully loaded into: {db['table']}')
        except SQLAlchemyError as e:
            logging.exception(f'Database load failed: {e}')

        finally:
            if 'engine' in locals():
                engine.dispose()

    # ---------------- Final Executer ---------------- #
    def run(self, override_config=None):
        logging.info('----- Pipeline Execution Started -----')
        if override_config:
            logging.info(f'Overriding config values: {override_config}')
            for key, value in override_config.items():
                self.data_paths[key] = value
        logging.info('Extracting Data...')
        sales = self.read_file(self.data_paths['csv_file'], 'csv')
        products = self.read_file(self.data_paths['json_file'], 'json')
        region = self.read_file(self.data_paths['parquet_file'], 'parquet')
        
        full_df = self.merge_data(sales, products, region)
        if not full_df.empty:
            logging.info('Loading Data...')
            self.save_output(full_df)
            self.load_to_db(full_df)
            logging.info('----- Pipeline Execution Completed -----')
        else:
            logging.warning('Merged Dataframe is empty. Nothing to save.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run configurable ETL pipeline')
    parser.add_argument('--config', type=str, default="../config/config.yaml", help='Path to config file')
    parser.add_argument('--csv_file', type=str, help='Override CSV path')
    parser.add_argument('--json_file', type=str, help='Overrid JSON path')
    parser.add_argument('--parquet_file', type=str, help='Override Parquet path')
    parser.add_argument('--output_file', type=str, help='Override output file path')
    parser.add_argument('--output_format', type=str, help='Override output file format')
    args = parser.parse_args()

    override = {k:v for k,v in vars(args).items() if v and k != 'config'}
    pipeline = Datapipeline(config_path=args.config)
    pipeline.run(override_config=override)