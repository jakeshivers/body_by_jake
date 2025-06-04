# bbj_dagster/schedules/bronze_schedule.py
from dagster import ScheduleDefinition
from bbj_dagster.jobs.bronze_job import bronze_job

bronze_daily_schedule = ScheduleDefinition(
    job=bronze_job,
    cron_schedule="0 4 * * *",  # Every day at 4:00 AM
    name="daily_bronze_schedule"
)
