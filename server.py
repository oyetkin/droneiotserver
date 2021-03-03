import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from pathlib import Path
from typing import List, Dict, Optional

from drivers import CSVAPI
import datetime

class SensorPayload(BaseModel):
    timestamp: int
    key:str
    unit:str
    value: float
    lat:Optional[float]
    lon:Optional[float]

path = "fake_database12.json"

app = FastAPI()
api = CSVAPI(path = path, schema = SensorPayload)

@app.post("/health")
def health():
    return {"status": "up"}

@app.post("/api/v0p1/sensor")
async def post_sensor(payload: SensorPayload):
    print(payload)
    api.write(payload)
    return {"status": "ok"}

@app.get("/api/v0p1/debug/get_data")
async def debug_get_all_data():
    data:List[SensorPayload] = api.get_data()
    return data

if __name__ == "__main__":
    uvicorn.run(app, host = "localhost", port = 8080)

