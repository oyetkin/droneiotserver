
import csv
import pydantic
import pathlib
from pydantic import BaseModel
from typing import List, Optional, Tuple
import csv

class CSVWriter:
    def __init__(self,
                path:str,
                schema:pydantic.BaseModel):
        self.path = path
        first_initialization = not pathlib.Path(path).exists()
        if first_initialization:
            pathlib.Path(path).touch()

        fr = open(path,'r') 
        self.reader = csv.reader(fr)
        self.cache = [row for row in self.reader]
        fr.close()
        
        
        self.schema = schema
        self.keys = list(self.schema.__fields__.keys())

        fw = open(path,'w', newline='')
        self.writer = csv.writer(fw)
        self.writer.writerows(self.cache)

        if first_initialization:
            self.writer.writerow(self.keys)

    def write(self, data:pydantic.BaseModel):

        keys = self.keys
        empty = ["" for x in range(len(keys))]
        present_keys = list(dict(data).keys())

        for i, k in enumerate(keys):
            if dict(data)[k] is not None:
                empty[i] = dict(data)[k]
        print(empty)
        self.writer.writerow(empty)

    def delete(self):
        self.cache = []
        del self.writer
        pathlib.Path(self.path).unlink()

        self.__init__(self.path, self.schema)


class CSVAPI:
    def __init__(self,
        path:str,
        schema:BaseModel):

        self.path   = path
        self.schema = schema
        self.writer = CSVWriter(path = path, schema= schema)
        self.state  = self.writer.cache[1:]

        __keys = self.writer.keys
        __data = []
        for line in self.state:
            kwargs = {__keys[i]:line[i] for i in range(len(__keys))}
            print(f">>>{kwargs}")
            __data.append(self.schema(**kwargs))

        self.state = __data
    
    def write(self, payload:BaseModel):
        self.schema.validate(payload)
        self.writer.write(payload)
        self.state.append(payload)
        print(self.state)
    def delete_in_memory(self):
        self.state = []

    def delete_disk(self):
        self.writer.delete()

    def get_data(self):
        return self.state

if __name__ == "__main__":
    class Test(BaseModel):
        a:str
        b:str
        c:str
        d:Optional[str]

    writer = CSVWriter( path="test.json",
                        schema= Test
                    )

    foo = Test(a = "a1",
               b = "b1",
               c = "c1", 
               d = "d1")

    bar = Test(a = "a2",
               b = "b2",
               c = "c2", 
               )
    writer.write(foo)
    writer.write(bar)
    writer.delete()
    writer.write(bar)
    writer.write(bar)

    writer2 = CSVAPI(path = "test2.json", schema = Test)
    writer2.write(foo)
    writer2.write(bar)
    writer2.write(bar)
    print(writer2.get_data())