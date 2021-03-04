import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from pydantic import BaseModel
from pathlib import Path
from typing import List, Dict, Optional
import datetime

from drivers import CSVAPI
from utils.general import load_yaml
from utils.fast import enable_cors

class SensorPayload(BaseModel):
    timestamp: int
    key:str
    unit:str
    value: float
    lat:Optional[float]
    lon:Optional[float]

path = "fake_database12.json"

app = FastAPI()
enable_cors(app)
api = CSVAPI(path = path, schema = SensorPayload)

@app.get("/health")
def health():
	return {"status":"up"}

#@app.post("/health")
#def health():
#	 return {"status": "up"}

@app.get("/")
def home():
	fs = open("static/front_page/index.html")
	content = fs.read()
	fs.close()
	return HTMLResponse( content )

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
	server_configs = load_yaml("configs/server_config.yaml")
	HOST = server_configs["host"]
	PORT = server_configs["port"]

	ssl_configs = load_yaml("configs/ssl_config.yaml")
	ssl_enabled = ssl_configs["enabled"]
	key_file = ssl_configs["key_path"] if ssl_enabled else None
	cert_file = ssl_configs["cert_path"] if ssl_enabled else None

	uvicorn.run(app,
				port=PORT,
				host=HOST,
				ssl_keyfile=key_file,
				ssl_certfile=cert_file
				)


