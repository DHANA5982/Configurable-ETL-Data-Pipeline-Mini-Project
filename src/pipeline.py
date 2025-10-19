from log import setup_logger
from read_csv import read_csv_file
from read_json import read_json_file
from read_parquet import read_parquet_file
from merge_data import merge_data

logger = setup_logger()

def main():
    logger.info("----- Pipeline Started -----")

    csv_path = "../data/sales.csv"
    json_path = "../data/products.json"
    parquet_path = "../data/region.parquet"

    sales_df = read_csv_file(csv_path)
    product_df = read_json_file(json_path)
    region_df = read_parquet_file(parquet_path)

    merged_df = merge_data(sales_df, product_df, region_df)
    if not merged_df.empty:
        output_path = "D:/GitHub/Big Data Engineer/ETL Mini Project/data/merged_data.csv"
        merged_df.to_csv(output_path, index=False)
        logger.info(f"Data merged successfully: {output_path}")
    else:
        logger.warning("Merged DataFrame is empty. Nothing to save.")

    logger.info("----- Pipeline Completed -----")


if __name__ == "__main__":
    main()
