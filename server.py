import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse

from pydantic import BaseModel
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Union
import datetime
import time  

from utils.general import load_yaml
from utils.fast import enable_cors
from utils.geojson_models import GeoJSONFeature, GeoJSONFeatureCollection, PointGeometry

from storage.sqlite_api.driver import MeasurementsDBAdapter
from storage.models import MeasurementsQuery, Measurement

#This is the input schema for a web request
class SensorPayload(BaseModel):
	key:str
	measurement_name:str 
	unit:str
	value: Union[float, int]
	timestamp: Optional[str]
	lat:Optional[float]
	lon:Optional[float]
	hardware:Optional[str]

class BatchedSensorPayload(BaseModel):
	key:str
	measurement_name:str
	unit:str
	values : List[Union[float, int]]
	timestamps: List[int]
	lat:Optional[float]
	lon:Optional[float]
	hardware:Optional[str]

def sensor_payload_to_measurement(payload:SensorPayload) -> Measurement:
	measurement = Measurement(key = payload.key,
							  measurement_name = payload.measurement_name,
							  unit = payload.unit,
							  timestamp = datetime.datetime.fromtimestamp(int(payload.timestamp)),
							  latitude = payload.lat,
							  longitude = payload.lon,
							  receipt_time = datetime.datetime.now(),
							  value = payload.value,
							  hardware = payload.hardware)
	return measurement

def batched_sensor_payload_to_measurements(batch: BatchedSensorPayload) -> List[Measurement]:
	measurements = []

	print(batch.timestamps)

	for value, timestamp in zip(batch.values, batch.timestamps):
		print(timestamp)
		measurement = Measurement(key=batch.key,
								  measurement_name=batch.measurement_name,
								  unit=batch.unit,
								  timestamp=datetime.datetime.fromtimestamp(int(timestamp)),
								  latitude=batch.lat,
								  longitude=batch.lon,
								  receipt_time = datetime.datetime.min,
								  value = value,
								  hardware=batch.hardware)
		measurements.append(measurement)
	return measurements


class BoundedQuery(BaseModel):
	time_range: Optional[Tuple[int, int]]
	lat_range: Optional[Tuple[float, float]]
	lon_range: Optional[Tuple[float, float]]
	key: Optional[str]
	unit: Optional[str]




storage_adapter = MeasurementsDBAdapter()

app = FastAPI()
enable_cors(app)

## passed
@app.post("/api/v0p2/sensor", tags=["Upload", "V0p2"])
async def post_sensor(payload: SensorPayload):
	"""
	Post sensor data.
	"""
	measurement = sensor_payload_to_measurement(payload)
	measurement.receipt_time = datetime.datetime.now()

	storage_adapter.insert_measurement(measurement)

	return JSONResponse(content = {"status" :"ok"})



##############################################################################
##############################################################################

## passed
@app.post("/api/v0p2/sensor/batch", tags=["Upload","V0p2"])
async def post_sensor_batch(payload: BatchedSensorPayload):
	"""
	Post sensor data in batches.
	"""
	measurements = batched_sensor_payload_to_measurements(payload)
	request_time = datetime.datetime.now()

	for m in measurements:
		m.receipt_time = request_time
		storage_adapter.insert_measurement(m)

	return {"status": "ok"}

#passed
@app.get("/api/v0p2/list_sensors", tags = ["Download", "V0p2"])
async def get_sensors():
	"""
	Returns a list of ALL unique sensors(by sensor key) and their latest latitude and longitude coordinates.
	"""
	sensors = storage_adapter.get_stations()

	response = []
	for name, latest_time, lat,lon in sensors:
		response.append(dict(key = name, lat = lat, lon = lon, latest = latest_time))

	return response

#passed
@app.get("/api/v0p2/list_stations", tags=["Download", "V0p2"])
async def get_stations():
	"""
	Returns a list of ALL unique sensors(by sensor key) and their latest latitude and longitude coordinates.
	"""
	stations = storage_adapter.get_stations()

	return list(map(lambda x: x.dict(), stations))


#passed
@app.get("/api/v0p2/list_stations/geojson", tags = ["Download", "V0p2"])
async def get_sensors_geojson():
	"""
	Returns a list of ALL unique sensors(by sensor key) and their latest latitude and longitude coordinates. The returned represenation is GEOJSON 
	"""	
	stations = storage_adapter.get_stations()

	feautures:List[GeoJSONFeature] = []

	for station  in stations:
		name, latest_time, lat, lon = station

		point_geom = PointGeometry(coordinates = [station.lon,
												  station.lat])

		geo_feature = GeoJSONFeature(properties = dict(key = station.station_key),
									geometry = point_geom)
		feautures.append(geo_feature)
	feature_collection = GeoJSONFeatureCollection(features = feautures)
	return feature_collection


#passed
@app.get("/api/v0p2/sensor_by_id/{sensor_id}", tags = ["Download"])
async def get_sensor_values(sensor_id: str,
							min_time: Optional[int]   = None,
							max_time: Optional[int]   = None,
							min_lat:  Optional[float] = None,
							max_lat:  Optional[float] = None,
							min_lon:  Optional[float] = None,
							max_lon:  Optional[float] = None,
							limit  :  Optional[int]   = None):


	int_to_ts = lambda x: datetime.datetime.fromtimestamp(int(x))

	lats = None if min_lat is None or max_lat is None else (min_lat, max_lat)
	lons = None if min_lon is None or max_lon is None else (min_lon, max_lon)
	time_range = None if min_time is None or max_time is None else ( int_to_ts(min_time),
																	 int_to_ts(max_time))

	print(time_range)

	query = MeasurementsQuery(key = sensor_id,
						 lats = lats,
						 lons = lons,
						 time_range = time_range)

	results = storage_adapter.get_bounded(query = query)
	return  list(results)


class APIStatus(BaseModel):
	up: bool
	connected_to_storage: bool 



@app.get("/health", tags= ["Auxilary"])
def health() -> APIStatus:
	"""
	Returns a status of the backend. Runs connectivity test to any storage drivers or components.  
	"""

	# With the CSV API we're going to assume nothing is severed from local storage.
	status = APIStatus(up = True,	connected_to_storage = True)
	return status


@app.get("/", tags= ["Auxilary"])
def home():
	"""
	Returns the front page to the IOT application.  
	"""
	fs = open("static/front_page/index.html")
	content = fs.read()
	fs.close()
	return HTMLResponse( content )


@app.get("/api/v0p1/debug/get_data", tags = ["Debug"])
async def debug_get_all_data():
	"""
	Returns ALL available data
	"""
	data:List[Measurement] = list(storage_adapter.get_all())
	return data

import time
@app.get("/api/v0p1/debug/get_timestamp", tags = ["Debug"])
async def debug_get_ts():
	"""
	Returns current timestamp. This is a feature used for debugging. 
	"""
	return {"timestamp": time.time()}

import time
@app.get("/api/v0p1/debug/get_dallas", tags = ["Debug"])
async def debug_get_dallas():
	"""
	Returns the coordinates of Dallas Texas. This is a feature used for debugging. It gives us the same set of coordinates each time it is called. 
	"""
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


