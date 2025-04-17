from typing import Any

import dlt

from .example_file_source import ExampleFileSource


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


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Run DLT pipeline with ExampleFileSource.")
    parser.add_argument("file_path", type=str, help="Path to the input file for ingestion.")
    args = parser.parse_args()

    try:
        source = ExampleFileSource(args.file_path)
        run_dlt_pipeline(source)
        print(f"Ingestion completed for file: {args.file_path}")
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
