from dagster import define_asset_job, AssetSelection

silver_job = define_asset_job(
    name="silver_job",
    selection=AssetSelection.groups("silver")
)
