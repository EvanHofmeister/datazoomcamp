Q1) From modifying the 'etl_web_to_gcs.py' file with the suggested perameters, we find the correct answer is 447,770 rows

[Q1. py file](web_to_gcs_hw3.py)

``` bash
CREATE OR REPLACE EXTERNAL TABLE dezoomcamp.fhv_2019
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://first_prefect_bucket_evan/data/fhv_tripdata_2019-*.parquet']
);
```
