# bbj_dagster/jobs/checkins_job.py

from dagster import define_asset_job

checkins_bronze_job = define_asset_job(
    name="checkins_bronze_job",
    selection=["checkins_bronze"]
)
