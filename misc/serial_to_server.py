import requests
from pydantic import BaseModel
from typing import Optional, Union
import time
import json

class SensorPayload(BaseModel):
	key:str
	measurement_name:str
	unit:str
	value: int
	timestamp: Optional[float]
	lat:Optional[float]
	lon:Optional[float]
	hardware:Optional[str]

class IOTStationUplink:
	def __init__(self,
				key:str,
				lat:float,
				lon:float):

		self.key = key
		self.lon = lon
		self.lat = lat
		self.endpoint = "http://api.is-conic.com/api/v0p1/sensor"

	def __get_station_params__(self):
		return dict(key = self.key, lat = self.lat, lon = self.lon)

	def post(self, measurement_name= "",
			 time_stamp = "",
			 hardware = "",
			 value:int = -9999,
			 unit:str = "None"):

		measurement = SensorPayload(measurement_name = measurement_name,
									timestamp = str(int(time_stamp)),
									hardware = hardware,
									value = int(value),
									unit = unit,
									**self.__get_station_params__())

		response = requests.post(self.endpoint, data= json.dumps(measurement.dict()))

		if response.status_code != 200:
			raise Exception(response.content)
		print(response.status_code)

if __name__ == "__main__":
	import serial

	station_link = IOTStationUplink(key="Demo Bouy Station", lat = 0.0, lon = 0.0)


	def postFromCSVLine(line:str):
		current_time = str(int(time.time()))
		red, green, blue, water, temp, pres, hum, voltage = line.split(",")
		station_link.post(value= red,     time_stamp = current_time, hardware="None", unit="ADC Value 16 bit", measurement_name="Red transmittance")
		station_link.post(value= green,   time_stamp = current_time, hardware="None", unit="ADC Value 16 bit", measurement_name="Green transmittance")
		station_link.post(value= blue,    time_stamp = current_time, hardware="None", unit="ADC Value 16 bit", measurement_name="Blue transmittance")
		station_link.post(value= water,   time_stamp = current_time, hardware="None", unit="ADC Value 16 bit", measurement_name="Water Temperature")
		station_link.post(value= temp,    time_stamp = current_time, hardware="None", unit="C", 			   measurement_name="Temperature")
		station_link.post(value= pres,    time_stamp = current_time, hardware="None", unit="hPa", 		       measurement_name="Pressure")
		station_link.post(value= hum,     time_stamp = current_time, hardware="None", unit="Percentage", 	   measurement_name="Humidity")
		station_link.post(value= voltage, time_stamp = current_time, hardware="None", unit="ADC Value 12 bit", measurement_name="Voltage")


	ser = serial.Serial('/dev/ttyUSB0')  # open serial port
	while True:
		line = ser.readline().strip()
		line.decode('ascii')
		postFromCSVLine(line)

