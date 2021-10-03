from pydantic import BaseModel
from typing import List, Dict

class PointGeometry(BaseModel):
	type: str = "Point"
	coordinates: List

class GeoJSONFeature(BaseModel):
	type: str = "Feature"
	properties: Dict = {}
	geometry: PointGeometry

class GeoJSONFeatureCollection(BaseModel):
	type: str = "FeatureCollection"
	features: List[GeoJSONFeature]