To answer these questions I needed to utilize the Prefect flow written for week 2:

[week-4-data-prep. py file](parameterized_flow-Homework-4-prep.py)

Q5) From running the below procedure/code we find the correct answer to be 61648442

``` bash
SELECT COUNT(*) FROM `ny-rides-evan.dbt_evan.fact_trips'
WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
```
![Q5](HW4_Q5.png)
