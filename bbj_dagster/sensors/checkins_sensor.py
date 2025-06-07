import os
from dagster import RunRequest, SensorEvaluationContext, sensor
from bbj_dagster.jobs.checkins_job import checkins_bronze_job
from bbj_dagster.utils.bronze_utils import get_new_success_partitions
from bbj_dagster.config.constants import  su
SUCCESS_DIR = "./tmp/success/checkins"  # 🆕 Replace /mnt/data/minio path

@sensor(job=checkins_bronze_job)
def checkins_success_sensor(context: SensorEvaluationContext):
    seen = context.cursor or ""
    seen_set = set(seen.split(",")) if seen else set()
    new_partitions = get_new_success_partitions(BRONZE_LOCAL + "/checkins", seen_set)

    if new_partitions:
        context.update_cursor(",".join(sorted(seen_set.union(new_partitions))))
        return [RunRequest(partition_key=dt) for dt in new_partitions]
    return []
