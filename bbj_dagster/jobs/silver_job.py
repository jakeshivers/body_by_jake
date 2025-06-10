# bbj_dagster/jobs/silver_job.py

from dagster import define_asset_job, AssetSelection

silver_job = define_asset_job(
    name="silver_job",
    selection=AssetSelection.groups("silver")  # This assumes your silver assets are grouped.
)
