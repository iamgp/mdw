import os
from collections.abc import Sequence
from typing import Any

import duckdb
from duckdb import DuckDBPyConnection
from fastapi import FastAPI, HTTPException
from loguru import logger
from pydantic import BaseModel

app = FastAPI(title="DuckDB Server")

# Initialize DuckDB connection
data_dir = os.getenv("DUCKDB_DATA_DIR", "/data")
db_path = os.path.join(data_dir, "warehouse.db")
conn: DuckDBPyConnection = duckdb.connect(db_path)


class QueryRequest(BaseModel):
    query: str
    parameters: dict[str, Any] | None = None


@app.post("/query")
async def execute_query(request: QueryRequest) -> list[dict[str, Any]]:
    """Execute a SQL query and return results as a list of dictionaries."""
    try:
        # Execute query with parameters if provided
        if request.parameters:
            result = conn.execute(request.query, request.parameters)
        else:
            result = conn.execute(request.query)

        # Convert result to list of dictionaries
        columns = result.description
        if columns:
            column_names = [col[0] for col in columns]
            rows: Sequence[tuple[Any, ...]] = result.fetchall()
            return [dict(zip(column_names, row, strict=False)) for row in rows]
        return []

    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check() -> dict[str, str]:
    """Check if the database is accessible."""
    try:
        result = conn.execute("SELECT 1")
        result.fetchone()
        return {"status": "healthy"}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=3457)
