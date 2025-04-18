"""
Sensors for Dagster pipelines.

This module defines sensors for triggering Dagster pipelines based on external events.
"""

from dagster import AssetSelection, define_asset_job, sensor

# Define a job that materializes the raw data asset
raw_data_job = define_asset_job(name="raw_data_job", selection=AssetSelection.keys("raw_data"))


@sensor(job=raw_data_job, minimum_interval_seconds=60)
def new_data_sensor(context):
    """
    Sensor that triggers when new data is available.

    This sensor checks for new data every minute and triggers a job to materialize
    the raw_data asset when new data is detected.

    In a real implementation, this would check for new files, database records,
    or API updates.
    """
    # This is a sample implementation. In a real scenario, we would:
    # 1. Check for new data (e.g., new files in S3, new database records)
    # 2. Return a RunRequest only if new data is found

    # For demonstration purposes, we'll simulate finding new data randomly
    # In a real implementation, remove this randomization and use actual checks
    # import random
    # if random.random() < 0.2:  # 20% chance of finding new data
    #     context.log.info("New data detected! Triggering raw_data_job.")
    #     return RunRequest(run_key=f"new_data_{int(time.time())}")
    # return None

    # For now, we'll just return a RunRequest every time for demonstration
    context.log.info("Checking for new data...")

    # Uncomment this in a real implementation with actual data checking
    # import time
    # return RunRequest(run_key=f"new_data_{int(time.time())}")

    # No new data found
    return None
