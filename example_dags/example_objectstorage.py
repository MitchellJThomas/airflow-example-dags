from __future__ import annotations

import pendulum
import requests

from airflow.sdk import ObjectStoragePath, dag, task

API = "https://opendata.fmi.fi/timeseries"

aq_fields = {
    "fmisid": "int32",
    "time": "datetime64[ns]",
    "AQINDEX_PT1H_avg": "float64",
    "PM10_PT1H_avg": "float64",
    "PM25_PT1H_avg": "float64",
    "O3_PT1H_avg": "float64",
    "CO_PT1H_avg": "float64",
    "SO2_PT1H_avg": "float64",
    "NO2_PT1H_avg": "float64",
    "TRSC_PT1H_avg": "float64",
}

base = ObjectStoragePath("s3://aws_default@airflow-example-data/")

@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def example_objectstorage():
    """
    ### Object Storage Example Documentation
    This is a example DAG to showcase the usage of the Object Storage API.
    Documentation that goes along with the Airflow Object Storage example is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/example/objectstorage.html)
    """
    
    @task
    def get_air_quality_data(**kwargs) -> ObjectStoragePath:
        """
        #### Get Air Quality Data
        This task gets air quality data from the Finnish Meteorological Institute's
        open data API. The data is saved as parquet.
        """
        import pandas as pd

        logical_date = kwargs["logical_date"]
        start_time = kwargs["data_interval_start"]

        params = {
            "format": "json",
            "precision": "double",
            "groupareas": "0",
            "producer": "airquality_urban",
            "area": "Uusimaa",
            "param": ",".join(aq_fields.keys()),
            "starttime": start_time.isoformat(timespec="seconds"),
            "endtime": logical_date.isoformat(timespec="seconds"),
            "tz": "UTC",
        }

        response = requests.get(API, params=params)
        response.raise_for_status()

        # ensure the bucket exists
        base.mkdir(exist_ok=True)

        formatted_date = logical_date.format("YYYYMMDD")
        path = base / f"air_quality_{formatted_date}.parquet"

        df = pd.DataFrame(response.json()).astype(aq_fields)
        with path.open("wb") as file:
            df.to_parquet(file)

        return path

    @task
    def analyze(path: ObjectStoragePath, **kwargs):
        """
        #### Analyze
        This task analyzes the air quality data, prints the results
        """
        import duckdb

        conn = duckdb.connect(database=":memory:")
        conn.register_filesystem(path.fs)
        conn.execute(f"CREATE OR REPLACE TABLE airquality_urban AS SELECT * FROM read_parquet('{path}')")

        df2 = conn.execute("SELECT * FROM airquality_urban").fetchdf()

        print(df2.head())


    obj_path = get_air_quality_data()
    analyze(obj_path)



example_objectstorage()

