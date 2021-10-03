from pydantic import BaseModel
from typing import List, Tuple, Dict, Optional, Union

from datetime import datetime

class Measurement(BaseModel):
	key: str
	measurement_name:str
	unit:str
	value:Union[int, float]
	timestamp:datetime
	receipt_time:datetime
	latitude:float
	longitude:float
	hardware:str

class MeasurementsQuery(BaseModel):
	lats:Optional[Tuple[float, float]]
	lons:Optional[Tuple[float, float]]
	time_range: Optional[Tuple[datetime, datetime]]
	receipt_time_range: Optional[Tuple[datetime,datetime]]
	measurement_name: Optional[str]
	hardware:Optional[str]
	key:Optional[str]
	unit:Optional[str]


# class StationContext(BaseModel):
# 	lat: float
# 	lon: float
# 	time: datetime
#
# class StationMeta(BaseModel):
# 	key: str
# 	latest: StationContext

class LatestStationMeta(BaseModel):
	lat: float
	lon: float
	latest_time: datetime
	station_key: str