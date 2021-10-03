#Data Architecture:

For now, a single table. These are the entries.  

```
key                 [String]    A name for the sensor. Like "Otto's sensor".     
measurement_name    [String]    Like "Temperature" 
unit                [String]    Like "Celcius"
value               [Number]    Float or Integer value
timestamp           [Integer]   timestamp. Epoch time  
receipt_time        [Integer]   Epoch time. 
lat                 [Float]     latitude coordinate
lon                 [Float]     longitude coordinate
hardware            [String]    Hardware name. Like sensor type. 
```
