import os
from typing import Any

import duckdb
from duckdb import DuckDBPyConnection
from fastapi import FastAPI, HTTPException
from loguru import logger
from pydantic import BaseModel

try:
    from duckdb import DuckDBPyResult  # type: ignore[attr-defined]
except ImportError:
    DuckDBPyResult = Any  # type: ignore

app = FastAPI(title="DuckDB Server")

# Initialize DuckDB connection
data_dir = os.getenv("DUCKDB_DATA_DIR", "/data")
db_path = os.path.join(data_dir, "warehouse.db")
conn: DuckDBPyConnection = duckdb.connect(db_path)  # type: ignore[reportUnknownMemberType]
# Pyright: DuckDB connect type is partially unknown due to dynamic typing in stubs.


class QueryRequest(BaseModel):
    query: str
    parameters: dict[str, Any] | None = None


@app.post("/query")
async def execute_query(request: QueryRequest) -> dict[str, list[dict[str, Any]]]:
    logger.info(f"Received DuckDB query: {request.query}")
    try:
        # Execute query with parameters if provided
        if request.parameters:
            result: Any = conn.execute(request.query, request.parameters)  # type: ignore
        else:
            result: Any = conn.execute(request.query)  # type: ignore
        # Pyright: DuckDBPyResult type is dynamic, fallback to Any.

        # Convert result to list of dictionaries
        rows: Any = result.fetchall()  # type: ignore
        # Pyright: fetchall() return type is dynamic.
        columns: Any = getattr(result, "description", None)  # type: ignore
        if columns:
            column_names = [col[0] for col in columns]
            records = [dict(zip(column_names, row, strict=False)) for row in rows]
            logger.info(f"DuckDB query executed successfully, {len(records)} rows returned")
            return {"result": records}
        logger.info("DuckDB query executed successfully, 0 rows returned")
        return {"result": []}

    except Exception as e:
        logger.error(f"DuckDB query failed: {e}")
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.get("/health")
async def health_check() -> dict[str, str]:
    try:
        result: Any = conn.execute("SELECT 1")  # type: ignore
        _ = result.fetchone()  # type: ignore
        # Pyright: DuckDBPyResult and fetchone() are dynamic.
        return {"status": "healthy"}
    except Exception as e:
        logger.error(f"Health check error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) from e


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=3457)
