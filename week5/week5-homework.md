Q1) From running the below procedure/code we find the correct answer to be 3.3.2

``` python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```

``` python
spark.version
```

Q2) From running the below procedure/code we find the correct answer to be 24MB

``` python
schema = types.StructType([
         types.StructField('dispatching_base_num',types.StringType(),True),
         types.StructField('pickup_datetime',types.TimestampType(),True),
         types.StructField('dropoff_datetime',types.TimestampType(),True),
         types.StructField('PULocationID',types.IntegerType(),True),
         types.StructField('DOLocationID',types.IntegerType(),True),
         types.StructField('SR_Flag',types.StringType(),True)]
)
```

``` python
df = spark.read\
    .option("header", "true") \
    .schema(schema) \
    .csv('data/fhvhv_tripdata_2021-06.csv')
df.createOrReplaceTempView("df")
df.head(5)
```

``` python
df.repartition(12)
```


Q3) From running the below procedure/code we find the correct answer to be 452,470

``` python
df.select('pickup_datetime').filter(df['pickup_datetime'].cast('date')=="2021-06-15").count()
```

Q4) From running the below procedure/code we find the correct answer to be 66.87 Hours

``` python
df.withColumn('trip_duration', (df.dropoff_datetime.cast('long') - df.pickup_datetime.cast('long'))/60/60) \
.sort('trip_duration', ascending=False) \
.show()
```

Q5) The answer can be found by simply looking at the forwarding port - 4040


Q6) From running the below procedure/code we find the correct answer to be 'Crown Heights North'

``` python
!wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv -O data/taxi_zones.csv
```

``` python
df_taxi_zones = spark.read.csv('data/taxi_zones.csv', header=True)
df_taxi_zones.createOrReplaceTempView("df_zones")
```

``` python
df_join = spark.sql("""
SELECT 
    Zone,
    count(1) as num_rides
FROM
    df a
JOIN df_zones b
ON a.PULocationID = b.LocationID
GROUP BY Zone
ORDER BY num_rides DESC
""")
df_join.show()
```
