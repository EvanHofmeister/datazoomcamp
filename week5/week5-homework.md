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
df = spark.read\
    .option("header", "true") \
    .schema(schema) \
    .csv('data/fhvhv_tripdata_2021-06.csv')

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


Q5) The answer can be found by simply looking at the forwarding port - 4040


Q6) From running the below procedure/code we find the correct answer to be 'Crown Heights North'

