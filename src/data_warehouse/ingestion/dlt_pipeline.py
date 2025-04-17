from typing import Any

import dlt


def run_dlt_pipeline(source: Any) -> None:
    """
    Run the DLT pipeline for the given source connector.
    Args:
        source: The data source connector (e.g., database, API, file system).
    Returns:
        None
    """
    # Guard clause for invalid source
    if source is None:
        raise ValueError("Source connector must be provided.")

    pipeline = dlt.pipeline(pipeline_name="data_ingestion", destination="duckdb", dataset_name="raw_data")
    # Placeholder: Replace with actual extraction logic
    data = source.extract()  # type: ignore[attr-defined]
    pipeline.run(data, table_name="ingested_data", write_disposition="replace")
