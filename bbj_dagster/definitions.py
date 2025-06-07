from dagster import Definitions
from bbj_dagster.assets.bronze_assets import checkins_bronze
from bbj_dagster.jobs.checkins_job import checkins_bronze_job
from bbj_dagster.sensors.checkins_sensor import checkins_success_sensor

defs = Definitions(
    assets=[checkins_bronze],
    jobs=[checkins_bronze_job],
    sensors=[checkins_success_sensor],
)