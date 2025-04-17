from typing import Any

from .source_base import SourceBase


class ExampleDBSource(SourceBase):
    """
    Example database source connector for DLT ingestion with incremental loading and CDC simulation.
    """

    def __init__(self, db_data: list[dict[str, Any]], last_id: int | None = None):
        self.db_data = db_data
        self.last_id = last_id

    def extract(self) -> list[dict[str, Any]]:
        # Simulate incremental loading: only return rows with id > last_id
        if self.last_id is None:
            return self.db_data
        return [row for row in self.db_data if row["id"] > self.last_id]

    def validate(self) -> bool:
        # Simulate DB connectivity check
        return True

    def get_state(self) -> int | None:
        # Return the max id as the new state after extraction
        if not self.db_data:
            return self.last_id
        return max(row["id"] for row in self.db_data)
