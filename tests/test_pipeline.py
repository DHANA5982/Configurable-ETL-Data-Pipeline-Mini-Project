import os
import pandas as pd
import pytest
from src_3.pipeline3 import Datapipeline

@pytest.fixture
def pipeline():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    return Datapipeline(config_path=config_path)

def test_config_load(pipeline):
    assert "data_paths" in pipeline.config

def test_read_file_csv(pipeline):
    df = pipeline.read_file("../data/sales.csv", "csv")
    assert isinstance(df, pd.DataFrame)

def test_read_file_json(pipeline):
    df = pipeline.read_file('../data/products.json', 'json')
    assert isinstance(df, pd.DataFrame)

def test_read_file_parquet(pipeline):
    df = pipeline.read_file('../data/region.parquet', 'parquet')
    assert isinstance(df, pd.DataFrame)

def test_merge_data(pipeline):
    sales = pd.DataFrame({"order_id": [1], "product_id": [101]})
    prod = pd.DataFrame({"product_id": [101], "name": ["Book"]})
    reg = pd.DataFrame({"order_id": [1], "region": ["West"]})
    merged = pipeline.merge_data(sales, prod, reg)
    assert "region" in merged.columns
    assert merged.shape[0] == 1

def test_save_output(pipeline, tmp_path):
    df = pd.DataFrame({"x": [1, 2]})
    pipeline.data_paths["output_file"] = os.path.join(tmp_path, "out.csv")
    pipeline.save_output(df)
    assert os.path.exists(pipeline.data_paths["output_file"])

def test_load_to_db(pipeline, tmp_path):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    pipeline.db_config["name"] = os.path.join(tmp_path, "test.db")
    pipeline.db_config["table"] = "test_table"
    pipeline.load_to_db(df)
    assert os.path.exists(pipeline.db_config["name"])
