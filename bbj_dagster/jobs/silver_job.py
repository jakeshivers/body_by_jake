# bbj_dagster/jobs/silver_job.py

from dagster import define_asset_job

silver_job = define_asset_job(name="silver_job")
