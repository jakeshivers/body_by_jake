# bbj_dagster/assets.py
from dagster import asset

@asset
def hello_asset():
    return "Hello, Dagster!"