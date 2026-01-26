## My answers to the first homework of data engineering zoomcamp

### [zoomcamp's link](https://github.com/DataTalksClub/data-engineering-zoomcamp) | [homework link](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/01-docker-terraform/homework.md)

---

### Homework 1 - Docker & Terraform

#### Qestion1. Understanding Docker images

```bash
$host> docker run -it --entrypoint "/bin/bash" python:3.13
$docker> pip --version
```

Output:

```bash
$dokcer> pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
```

#### Question 2. Understanding docker networking and docker-compose

Answer:

> postgres:5432 or db:5432

#### Question 3. Counting short trips

Answer:

> 8007

```SQL
SELECT COUNT(trip_distance) AS total_records
FROM green_taxi_data
WHERE lpep_pickup_datetime BETWEEN ('2025-11-01'AND '2025-12-01') AND trip_distance <= 1;
```

#### Question 4. Longest trip for each day

Answer:

> 2025-11-14

```SQL
SELECT lpep_pickup_datetime, trip_distance
FROM green_taxi_data
WHERE trip_distance < 100
ORDER BY trip_distance DESC
LIMIT 1
```

#### Question 5. Popular pickup locations in November 2025

Answer:

> East Harlem South

```SQL
select (select count(*) from green_taxi_data where date(lpep_pickup_datetime) = '2025-11-18' and "PULo
 cationID" = 74) as East_Harlem_North, (select count(*) from green_taxi_data where date(lpep_pickup_datetime) = '2025-11-18' a
 nd "PULocationID" = 75) as East_Harlem_South, (select count(*) from green_taxi_data where date(lpep_pickup_datetime) = '2025-
 11-18' and "PULocationID" = 95) as forest_hills, (select count(*) from green_taxi_data where date(lpep_pickup_datetime) = '20
 25-11-18' and "PULocationID" = 166) as Morningside_Heights from green_taxi_data limit 1
```
