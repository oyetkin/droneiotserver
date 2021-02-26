import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

@app.post("/health")
def health():
    return {"status": "up"}

class SensorPayload(BaseModel):
    timestamp: int
    key:str
    value: float

@app.post("/api/v0p1/sensor")
async def post_sensor(request: SensorPayload):
    print(request)
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host = "0.0.0.0", port = "8080")