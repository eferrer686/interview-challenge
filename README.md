# Interview Challenge

This repo contains the Data Engineering challenge.


### The solution is split on the folowing areas
 
1. Python code
2. Postgres tables
3. Files
4. Docker compose

## 1. Python Code

- The scripts can be found on the `dags/include` path.
- One DAG was created for this solution called `challenge_dag` and can be found on the `dags` path.

## 2. Postgres tables

- A SQL script can found on the `dags/include/create_tables.sql` path.
- Each execution the DAG will setup de DB to ensure the scripts can run properly.
- There are 4 tables: 
  - pronosticopormunicipiosgz_raw: log of all queried data with timestamp
  - pronosticopormunicipiosgz_latest: latest queried data
  - latest_temp_avg_by_mun: average of temp_min and temp_max of the last 2 hours of queried data
  - datos_municipios: loaded data from customers
    - Data found on path: `data/data_municipios`
- Postgres can be queried using the following connection string:
```python
# Inside docker
host='postgres' dbname='airflow' user='airflow' password='airflow'
# Local machine
host='localhost' dbname='airflow' user='airflow' password='airflow'
```


## 3. Files

As needed files are generated with the following data:
- `data/datacsv`: contains the daily logs of data, same as `pronosticopormunicipiosgz_raw` table.
- `data/datacsv/latest.csv`: contains last queried data same as `pronosticopormunicipiosgz_latest` table.
- `data/current/data.csv`: contains the joined data from `pronosticopormunicipiosgz_latest` table and the customer data found on `data/data_municipios`.
- All files are updated hourly.


## 4. Docker Compose and Init

This project is contained on a docker image that can be created using docker compose.
```bash
 docker-compose up airflow-init
 docker-compose up
```


