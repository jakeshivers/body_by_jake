# bbj_dagster/__init__.py
from dagster import Definitions
from dagster import Definitions
from bbj_dagster.jobs.bronze_job import bronze_job
from bbj_dagster.schedules.bronze_schedule import bronze_daily_schedule
from bbj_dagster.assets.bronze_assets import (
    members_bronze,
    checkins_bronze,
    facility_usage_bronze,
    cancellations_bronze,
    retail_bronze,
)

defs = Definitions(
    assets=[
        members_bronze,
        checkins_bronze,
        facility_usage_bronze,
        cancellations_bronze,
        retail_bronze,
    ],
    jobs=[bronze_job],
    schedules=[bronze_daily_schedule],
)
