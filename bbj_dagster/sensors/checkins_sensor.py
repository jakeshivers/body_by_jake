import os
from dagster import RunRequest, SensorEvaluationContext, sensor
from bbj_dagster.jobs.checkins_job import checkins_bronze_job

SUCCESS_DIR = "./tmp/success/checkins"  # 🆕 Replace /mnt/data/minio path

@sensor(job=checkins_bronze_job)
def checkins_success_sensor(context: SensorEvaluationContext):
    seen = context.cursor or ""
    seen_set = set(seen.split(",")) if seen else set()

    new_partitions = []
    if not os.path.isdir(SUCCESS_DIR):
        return []

    for subdir in os.listdir(SUCCESS_DIR):
        path = os.path.join(SUCCESS_DIR, subdir, "_SUCCESS")
        if os.path.isfile(path) and subdir not in seen_set:
            new_partitions.append(subdir)

    if new_partitions:
        new_cursor = ",".join(sorted(seen_set.union(new_partitions)))
        context.update_cursor(new_cursor)
        return [RunRequest(partition_key=dt) for dt in new_partitions]

    return []
