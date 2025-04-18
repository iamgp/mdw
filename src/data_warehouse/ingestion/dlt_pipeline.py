import logging
import time
from collections.abc import Sized
from typing import TypeVar, cast

import dlt

from .example_db_source import ExampleDBSource
from .example_file_source import ExampleFileSource
from .source_base import SourceBase

T = TypeVar("T")


def run_dlt_pipeline(source: SourceBase[T], max_retries: int = 3, retry_delay: float = 2.0) -> None:
    """
    Run the DLT pipeline for the given source connector with logging and retry logic.
    Args:
        source: The data source connector (e.g., database, API, file system).
        max_retries: Maximum number of retries for transient errors.
        retry_delay: Delay (seconds) between retries.
    Returns:
        None
    """
    logger = logging.getLogger("dlt_pipeline")
    logger.info(f"Starting DLT pipeline for source: {type(source).__name__}")

    attempt = 0
    while attempt < max_retries:
        try:
            pipeline = dlt.pipeline(pipeline_name="data_ingestion", destination="duckdb", dataset_name="raw_data")
            data: T = source.extract()
            if hasattr(data, "__len__"):
                logger.info(f"Extracted {len(cast(Sized, data))} records from source.")
            else:
                logger.warning("Extracted data does not support len().")
            pipeline.run(data, table_name="ingested_data", write_disposition="replace")
            logger.info("Pipeline run completed successfully.")
            return
        except Exception as exc:
            attempt += 1
            logger.error(f"Pipeline run failed on attempt {attempt}: {exc}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.critical("Max retries reached. Pipeline failed.")
                raise


if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

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

            db_data.append({"id": 4, "name": "Dave"})
            print("Second run: ingest only new data")
            db_source_incremental = ExampleDBSource(db_data, last_id=new_state)
            print(f"State before second run: {db_source_incremental.last_id}")
            run_dlt_pipeline(db_source_incremental)
            print(f"State after second run: {db_source_incremental.get_state()}")
        else:
            raise ValueError("Invalid mode")
    except Exception as exc:
        logging.critical(f"Error: {exc}")
        sys.exit(1)
