"""
Schedules for Dagster pipelines.

This module defines schedules for automatically materializing Dagster assets on a regular basis.
"""

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

# Define a job that includes all assets
all_assets_job = define_asset_job(name="all_assets_job", selection=AssetSelection.all())

# Define a daily schedule for materializing all assets
daily_schedule = ScheduleDefinition(
    name="daily_refresh",
    cron_schedule="0 5 * * *",  # Run at 5:00 AM every day
    job=all_assets_job,
    description="Materialize all assets daily at 5:00 AM",
)
