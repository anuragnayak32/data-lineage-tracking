from fastapi import FastAPI
from app.routes import router
from app.models import create_table

app = FastAPI(title="Data Lineage Tracking API")

create_table()

app.include_router(router)

print("🚀 FastAPI server is running")
print("📘 Swagger UI available at: http://127.0.0.1:8000/docs")

