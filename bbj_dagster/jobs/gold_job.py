# bbj_dagster/jobs/gold_job.py
from dagster import define_asset_job, AssetSelection

gold_job = define_asset_job(
    name="gold_job",
    selection=AssetSelection.groups("gold")
)
