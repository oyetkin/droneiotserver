from sqlalchemy import Table, Column, Integer, String, MetaData, Numeric, DateTime, Float
from sqlalchemy import or_, and_
from sqlalchemy import select
from sqlalchemy import create_engine
from datetime import datetime
from sqlalchemy import bindparam

from pydantic import BaseModel
from typing import List, Dict, Tuple, Union, Iterable, Optional


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

class MeasurementsDBAdapter:
	def __init__(self):
		"""
		  Measurements:
		  key                 [String]    A name for the sensor. Like "Otto's sensor".
		  measurement_name    [String]    Like "Temperature"
		  unit                [String]    Like "Celcius"
		  value               [Number]    Float or Integer value
		  timestamp           [Integer]   timestamp. Epoch time
		  receipt_time        [Integer]   Epoch time.
		  lat                 [Float]     latitude coordinate
		  lon                 [Float]     longitude coordinate
		  hardware            [String]    Hardware name. Like sensor type.
		"""

		engine = create_engine('sqlite:///measurements.db', echo=True)

		meta = MetaData()

		self.measurements = Table(
            'measurements', meta,
            Column('id', Integer, primary_key = True),
            Column('key', String),
            Column('measurement_name', String),
            Column('unit', String),
            Column('value', Numeric),
            Column('timestamp', DateTime),
            Column('receipt_time', DateTime),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('hardware',  String)
         )

		meta.create_all(engine)
		self.conn = engine.connect()

	def insert_measurement(self, m:Measurement) -> None:
		insert = self.measurements.insert().values(**m.dict())
		result = self.conn.execute(insert)

	def insert_measurements(self, m:List[Measurement]) -> None:
		measurements_as_dicts = [x.dict() for x in m]
		insert = self.measurements.insert().values(measurements_as_dicts)
		result = self.conn.execute(insert)


	def get_all(self) -> Iterable[Measurement]:
		s = self.measurements.select()
		result = self.conn.execute(s)
		return map(lambda x: Measurement(**x), result)


	def get_bounded(self, query:MeasurementsQuery) -> Iterable[Measurement]:

		# lat1, lat2 = sorted(lats)

		selection_criteria = []

		#### Ranged
		if query.lats is not None:
			lat1, lat2 = sorted(query.lats)
			selection_criteria.append( self.measurements.c.latitude > lat1)
			selection_criteria.append( self.measurements.c.latitude < lat2)

		if query.lons is not None:
			lons1, lons2 = sorted(query.lons)
			selection_criteria.append( self.measurements.c.longitude > lons1)
			selection_criteria.append( self.measurements.c.longitude < lons2)

		if query.time_range is not None:
			t1, t2 = sorted(query.time_range)
			selection_criteria.append( self.measurements.c.timestamp > t1)
			selection_criteria.append( self.measurements.c.timestamp < t2)

		if query.receipt_time_range is not None:
			t1, t2 = sorted(query.receipt_time_range)
			selection_criteria.append( self.measurements.c.receipt_time > t1)
			selection_criteria.append( self.measurements.c.receipt_time < t2)

		### Exact match
		if query.measurement_name is not None:
			selection_criteria.append(self.measurements.c.measurement_name == query.measurement_name)

		if query.key is not None:
			selection_criteria.append(self.measurements.c.key == query.key)

		if query.unit is not None:
			selection_criteria.append(self.measurements.c.unit == query.unit)

		if query.hardware is not None:
			selection_criteria.append(self.measurements.c.hardware == query.hardware)

		sel = self.measurements.select().where(and_(*selection_criteria))

		result = self.conn.execute(sel)
		return map(lambda x: Measurement(**x), result)


if __name__ == "__main__":
	adapter = MeasurementsDBAdapter()


	import random
	import time
	from datetime import  datetime, timedelta

	def __generate_random_measurement() -> Measurement:
		measurement =  Measurement(key = f"Otto's station No. {int((random.random() *2 )//1)}", measurement_name = "Temperature",
                                  unit = "C",
                                  value = random.random()*50,
                                  timestamp = datetime.now(),
                                  receipt_time = datetime.now(),
                                  latitude = 0.0 + (random.random()-.5) * 10,
                                  longitude = 0.0 + (random.random()-.5) * 10,
                                  hardware = f"Thermometer {(random.random() *3)//1}"
                                  )
		return measurement


	# for x in range(10000):
	# 	adapter.insert_measurement(__generate_random_measurement())

	def time_and_print(name, query:MeasurementsQuery):
		t1 = time.time()
		x = adapter.get_bounded(query)
		xx = list(x)
		t2 = time.time()
		print(f"{name}:::")
		print(t2 - t1)
		print(f"Len: {len(xx)}")
		#print(*xx[:10], sep = "\n")
		print()


	time_and_print(name="Normal", query=MeasurementsQuery())
	time_and_print(name="Lat Bounded", query= MeasurementsQuery(lats = (0,10)))
	time_and_print(name="Hardware Bounded", query=MeasurementsQuery(hardware = "Thermometer 3"))
	time_and_print(name="Key check", query=MeasurementsQuery(key = "Otto's station"))


	insert = __generate_random_measurement()
	print(insert)
	adapter.insert_measurement(insert)

	print("sleeping")
	time.sleep(3)
	print(f"::::{insert.receipt_time}")
	now = datetime.now()
	before = now - timedelta(minutes = 10)
	print(f"--{before}")
	print(f"--{now}")

	time_and_print(name="Date check", query=MeasurementsQuery(receipt_time_range = (before, now)))



# tf_1 = time.time()
	# foo = adapter.get_all()
	# l_foo = list(foo)
	# tf_2 = time.time()
	#
	#
	# tb_1 = time.time()
	# bar = adapter.get_bounded(MeasurementsQuery(lats = (0,10)))
	# l_bar = list(bar)
	# tb_2 = time.time()
	#
	# print("All")
	# print(tf_2 - tf_1)
	# print(f"Len: {len(l_foo)}")
	# print(*l_foo[:10], sep = "\n")
	#
	# print()
	# print("Select")
	# print(tb_2 - tb_1)
	# print(f"Len: {len(l_bar)}")
	# print(*l_bar[:10], sep="\n")

#
		#
		# print("Bounded")


#         return measurement
#
#     import 		pandas as pd
#
# 		results = []
# 		for _ in range(10000):
#
# 			for x in range(10):
# 				measurement = __generate_random_measurement()
# 				adapter.insert_measurement(measurement)
#
# 			import time
#
# 			t1 = time.time()
# 			stuff = list(adapter.get_all())
# 			t2 = time.time()
#
# 			# print("------")
# 			# print(f"dict(length = {len(stuff)}, time={t2 - t1}),")
#
# 			results.append(dict(length=len(stuff), time=t2 - t1))
#
# df = pd.DataFrame(results)
#     df.to_csv("yoink.csv")