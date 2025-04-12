from fastapi import FastAPI

app = FastAPI(
    title="Data Warehouse",
    description="Modern Data Warehouse Platform",
    version="0.1.0",
)


@app.get("/")
async def root():
    return {"message": "Data Warehouse API is running"}
