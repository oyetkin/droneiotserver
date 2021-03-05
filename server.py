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
    timestamp: Optional[int]
    key:str
    unit:str
    value: float
    lat:Optional[float]
    lon:Optional[float]

class SensorBatch(BaseModel):
	items:List[SensorPayload]

class BoundedQuery(BaseException):
	time_range: Optional[Tuple[int, int]]
	lat_range: Optional[Tuple[float, float]]
	lon_range: Optional[Tuple[float, float]]
	key: Optional[str]
	unit: Optional[str]
	# unit_regex: Optional[str]
	# key_regex: Optional[str]


path = "fake_database12.json"

app = FastAPI()
enable_cors(app)
api = CSVAPI(path = path, schema = SensorPayload)

@app.post("/api/v0p1/sensor")
async def post_sensor(payload: SensorPayload):
	"""
	Post sensor data.
	"""

    print(payload)
    api.write(payload)
    return {"status": "ok"}


@app.post("/api/v0p1/sensor/batch")
async def post_sensor_batch(payload: SensorBatch):
	"""
	Post sensor data.
	"""
	for item in payload.items:
		print(payload)
		api.write(payload)
	return {"status": "ok"}

@app.get("/api/v0p1/query/bounded")
async def bounded_query(payload: BoundedQuery):
#   Idea: Make ranges bounded in one direction only
    data:List[SensorPayload] = api.get_data()
	keys:List[str] = list(dict(data).keys())

	if "time_range" in keys:
		in_range = lambda d: d.timestamp < payload.time_range[1] && d.timestamp > payload.time_range[0] 
		data = filter(in_range, data)
	
	if "lat_range" in keys:
		in_range = lambda d: d.lat < payload.lat_range[1] && d.lat > payload.lat_range[0] 
		data = filter(in_range, data)

	if "lon_range" in keys:
		in_range = lambda d: d.lon < payload.lon_range[1] && d.lon > payload.lon_range[0] 
		data = filter(in_range, data)

	if "key" in keys:
		has_key = lambda d: payload.key in d  
		data = filter(has_key, data)

	if "unit" in keys"
		has_unit = lambda d: playload.unit in d
		data = filter(has_unit, data)	
	
	return list(data)

@app.get("/health")
def health():
	return {"status":"up"}

@app.get("/")
def home():
	fs = open("static/front_page/index.html")
	content = fs.read()
	fs.close()
	return HTMLResponse( content )


@app.get("/api/v0p1/debug/get_data")
async def debug_get_all_data():
    data:List[SensorPayload] = api.get_data()
    return data

import time
@app.get("/api/v0p1/debug/get_timestamp")
async def debug_get_ts():
    return {"timestamp": time.time()}

import time
@app.get("/api/v0p1/debug/get_dallas")
async def debug_get_dallas():
    return {"lat": 32.7767 , "lon": -96.7970}


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


