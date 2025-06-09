# bbj_dagster/assets/silver/__init__.py

from .members_silver import members_silver
from .checkins_silver import checkins_silver
from .cancellations_silver import cancellations_silver
from .retail_silver import retail_silver
from .facility_usage_silver import facility_usage_silver

assets = [
    members_silver,
    checkins_silver,
    cancellations_silver,
    retail_silver,
    facility_usage_silver,
]
