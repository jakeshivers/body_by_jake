
from dagster import Definitions

from bbj_dagster.jobs.bronze_job import bronze_job
from bbj_dagster.jobs.silver_job import silver_job
from bbj_dagster.jobs.gold_job import gold_job

from bbj_dagster.schedules.bronze_schedule import bronze_daily_schedule

# bronze
from bbj_dagster.assets.bronze import assets as bronze_assets

# silver
from bbj_dagster.assets.silver import assets as silver_assets

#gold
from bbj_dagster.assets.gold import assets as gold_assets

all_assets = bronze_assets + silver_assets + gold_assets

for i, a in enumerate(all_assets):
    print(f"[DEBUG] Asset {i}: {a} ({type(a)})")

defs = Definitions(
        assets=all_assets,
        jobs=[bronze_job, silver_job, gold_job],
        schedules=[bronze_daily_schedule],  #only have a bronze job for now. Silver should be a dependency 
 )