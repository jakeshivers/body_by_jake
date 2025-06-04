# bbj_dagster/jobs/bronze_job.py
from dagster import define_asset_job

bronze_job = define_asset_job(name="bronze_job")
