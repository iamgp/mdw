from typing import Any

import dlt

from .example_db_source import ExampleDBSource
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

    parser = argparse.ArgumentParser(description="Run DLT pipeline with ExampleFileSource or ExampleDBSource.")
    parser.add_argument("mode", choices=["file", "db"], help="Source type: file or db.")
    parser.add_argument("source_arg", type=str, help="File path (for file) or 'demo' for db mode.")
    args = parser.parse_args()

    try:
        if args.mode == "file":
            source = ExampleFileSource(args.source_arg)
            run_dlt_pipeline(source)
            print(f"Ingestion completed for file: {args.source_arg}")
        elif args.mode == "db":
            # Simulate DB data and incremental loading
            db_data = [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Carol"},
            ]
            print("First run: ingest all data")
            db_source = ExampleDBSource(db_data)
            print(f"Initial state: {db_source.get_state()}")
            run_dlt_pipeline(db_source)
            new_state = db_source.get_state()
            print(f"State after first run: {new_state}")

            # Simulate new data for incremental load
            db_data.append({"id": 4, "name": "Dave"})
            print("Second run: ingest only new data")
            db_source_incremental = ExampleDBSource(db_data, last_id=new_state)
            print(f"State before second run: {db_source_incremental.last_id}")
            run_dlt_pipeline(db_source_incremental)
            print(f"State after second run: {db_source_incremental.get_state()}")
        else:
            raise ValueError("Invalid mode")
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
