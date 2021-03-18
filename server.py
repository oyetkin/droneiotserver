import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse

from pydantic import BaseModel
from pathlib import Path
from typing import List, Dict, Optional, Tuple
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

class BoundedQuery(BaseModel):
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
	api.write(payload)
	return JSONResponse(content = {"status" :"ok"})


# class GetSensorQuery(BaseModel):
# 	key: Optional[str]
# 	min_lat: Optional[float]
# 	max_lat: Optional[float]
# 	min_lon: Optional[float]
# 	max_lon: Optional[float]

@app.get("/api/v0p1/list_sensors")
async def get_sensors():
	"""
	get sensors data.
	"""
	data:List[SensorPayload] = api.get_data()
	sensor_dict = {d.key: d for d in data}
	sensor_data = list(sensor_dict.values())
	print(sensor_data)
	return [dict(key = s.key, lat = s.lat, lon = s.lon) for s in sensor_data]



class PointGeometry(BaseModel):
	type:str = "Point"
	coordinates: List
	
class GeoJSONFeature(BaseModel):
	type:str = "Feature"
	properties: Dict = {}
	geometry: PointGeometry
	
class GeoJSONFeatureCollection(BaseModel):
	type:str = "FeatureCollection"
	features: List[GeoJSONFeature]

@app.get("/api/v0p1/list_sensors/geojson")
async def get_sensors():
	
	data:List[SensorPayload] = api.get_data()
	sensor_dict = {d.key: d for d in data}
	sensor_data = list(sensor_dict.values())
	
	feautures:List[GeoJSONFeature] = []

	for sensor in sensor_data:
		point_geom = PointGeometry(coordinates = [sensor.lon, sensor.lat])
		geo_feature = GeoJSONFeature(properties = dict(key = sensor.key),
									geometry = point_geom)
		feautures.append(geo_feature)
	print(feautures)
	feature_collection = GeoJSONFeatureCollection(features = feautures)
	return feature_collection#[dict(key = s.key, lat = s.lat, lon = s.lon) for s in sensor_data]


@app.get("/api/v0p1/sensor_by_id/{sensor_id}")
async def get_sensor_values(sensor_id: str,
							min_time: Optional[int]   = None,
							max_time: Optional[int]   = None,
							min_lat:  Optional[float] = None,
							max_lat:  Optional[float] = None, 
							min_lon:  Optional[float] = None,
							max_lon:  Optional[float] = None):

	data:List[SensorPayload] = api.get_data()
	data = [sensor for sensor in data if sensor.key == sensor_id]

	print(min_lat,	type(min_lat))
	print(min_time, type(min_time))

	if min_time is not "":
		data = [sensor for sensor in data if sensor.timestamp > min_time]
	if max_time is not "":
		data = [sensor for sensor in data if sensor.timestamp < max_time]
	
	if min_lat is not "":
		data = [sensor for sensor in data if sensor.lat > min_lat]
	if max_lat is not "":
		data = [sensor for sensor in data if sensor.lat < max_lat]
	
	if min_lon is not "":
		data = [sensor for sensor in data if sensor.lon > min_lon]
	if max_lon is not "":
		data = [sensor for sensor in data if sensor.lon > max_lon]
	
	data = sorted(data, key = lambda sensor: -sensor.timestamp)
	return data

	
# @app.post("/api/v0p1/sensor/batch")
# async def post_sensor_batch(payload: SensorBatch):
# 	"""
# 	Post sensor data.
# 	"""
# 	for item in payload.items:
# 		print(payload)
# 		api.write(payload)
# 	return {"status": "ok"}

# @app.post("/api/v0p1/query/bounded")
# async def bounded_query(payload:BoundedQuery):
# 	#   Idea: Make ranges bounded in one direction only
# 	data:List[SensorPayload] = api.get_data()
# 	keys:List[str] = list(dict(payload).keys())

# 	if bounded_query.time_range is not None and len(bounded_query.time_range) == 2:
# 		in_range = lambda d: d.timestamp < payload.time_range[1] and d.timestamp > payload.time_range[0] 
# 		data = filter(in_range, data)

# 	if bounded_query.lat_range is not None and len(bounded_query.lat_range) == 2:
# 		in_range = lambda d: d.lat < payload.lat_range[1] and d.lat > payload.lat_range[0] 
# 		data = filter(in_range, data)

# 	if bounded_query.lon_range is not None and len(bounded_query.lon_range) == 2:
# 		in_range = lambda d: d.lon < payload.lon_range[1] and d.lon > payload.lon_range[0] 
# 		data = filter(in_range, data)

# 	if bounded_query.key is not None:
# 		has_key = lambda d: payload.key in d
# 		data = filter(has_key, data)

# 	if bounded_query.unit is not None:
# 		has_unit = lambda d: playload.unit in d
# 		data = filter(has_unit, data)

# 	return list(data)

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


