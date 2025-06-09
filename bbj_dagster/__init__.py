# bbj_dagster/__init__.py
from dagster import Definitions
from bbj_dagster.jobs.bronze_job import bronze_job
from bbj_dagster.schedules.bronze_schedule import bronze_daily_schedule
from bbj_dagster.assets.bronze.bronze_assets import (
    members_bronze,
    checkins_bronze,
    facility_usage_bronze,
    cancellations_bronze,
    retail_bronze,
)
from bbj_dagster.assets.silver.checkins_silver import checkins_silver
from bbj_dagster.jobs.silver_job import silver_job
from bbj_dagster.assets.silver import assets as silver_assets
from bbj_dagster.assets.silver.cancellations_silver import cancellations_silver



defs = Definitions(
    assets=[
        #bronze
        members_bronze,
        checkins_bronze,
        facility_usage_bronze,
        cancellations_bronze,
        retail_bronze,

        #silver
        checkins_silver,
        cancellations_silver,
        *silver_assets,
        

        #gold
    ],
    jobs=[bronze_job, silver_job],
    schedules=[bronze_daily_schedule],  #only have a bronze job for now. Silver should be a dependency 
)
