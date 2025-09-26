from __future__ import annotations

import pendulum
import requests
import pandas as pd
from io import BytesIO
from typing import Dict, Any
import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Configuration
API = "https://opendata.fmi.fi/timeseries"
S3_BUCKET = Variable.get("s3_bucket", default_var="airflow-example-data")
S3_PREFIX = Variable.get("s3_prefix", default_var="air_quality_data/")
AWS_CONN_ID = Variable.get("aws_conn_id", default_var="aws_default")

# Data schema with validation
AQ_FIELDS = {
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

# Air Quality Index thresholds
AQI_THRESHOLDS = {
    "good": 50,
    "moderate": 100,
    "unhealthy_sensitive": 150,
    "unhealthy": 200,
    "very_unhealthy": 300,
    "hazardous": 500,
}

@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "s3", "data-processing", "air-quality", "best-practices"],
    doc_md=__doc__,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=30),
        "email_on_failure": True,
        "email_on_retry": False,
    },
    max_active_tasks=3,
    max_active_runs=1,
    description="Air Quality Data Processing with S3 Storage - Best Practices Implementation",
)
def example_objectstorage_operators():
    """
    ### Air Quality Data Processing with S3 Operators - Best Practices
    
    This DAG demonstrates Airflow 3.0 best practices for data processing:
    
    **Key Features:**
    - ✅ Resource pool management
    - ✅ Comprehensive error handling
    - ✅ Data quality validation
    - ✅ Structured logging
    - ✅ Task grouping
    - ✅ Configuration management
    - ✅ Monitoring and alerting
    
    **Data Flow:**
    1. **Connection Validation** - Validate AWS S3 connection and bucket access
    2. **Data Ingestion** - Fetch air quality data from FMI API
    3. **Data Validation** - Validate data quality and completeness
    4. **Data Storage** - Store validated data in S3
    5. **Data Analysis** - Perform statistical analysis
    """
    
    def get_aqi_level(aqi_value: float) -> str:
        """Determine air quality level based on AQI value."""
        if aqi_value < AQI_THRESHOLDS["good"]:
            return "Good"
        elif aqi_value < AQI_THRESHOLDS["moderate"]:
            return "Moderate"
        elif aqi_value < AQI_THRESHOLDS["unhealthy_sensitive"]:
            return "Unhealthy for Sensitive Groups"
        elif aqi_value < AQI_THRESHOLDS["unhealthy"]:
            return "Unhealthy"
        elif aqi_value < AQI_THRESHOLDS["very_unhealthy"]:
            return "Very Unhealthy"
        else:
            return "Hazardous"
    
    # Connection Validation Task
    @task(
        retries=1,
        retry_delay=pendulum.duration(minutes=1),
    )
    def validate_aws_connection():
        """
        #### Validate AWS Connection
        Validate that the AWS connection is properly configured and accessible.
        """
        logger = logging.getLogger(__name__)
        
        try:
            logger.info(f"🔍 Validating AWS connection: {AWS_CONN_ID}")
            
            # Test S3Hook initialization
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            logger.info(f"✅ S3Hook initialized with connection: {AWS_CONN_ID}")
            
            # Test connection by listing buckets (limited to 1 for efficiency)
            try:
                buckets = s3_hook.list_buckets()
                bucket_names = [bucket['Name'] for bucket in buckets]
                logger.info(f"✅ AWS connection successful. Found {len(bucket_names)} buckets")
                logger.info(f"📦 Available buckets: {bucket_names[:5]}{'...' if len(bucket_names) > 5 else ''}")
                
                # Check if target bucket exists
                if S3_BUCKET in bucket_names:
                    logger.info(f"✅ Target bucket '{S3_BUCKET}' found")
                else:
                    logger.warning(f"⚠️ Target bucket '{S3_BUCKET}' not found in available buckets")
                    logger.info("💡 Please create the bucket or update the s3_bucket variable")
                
                return {
                    "connection_id": AWS_CONN_ID,
                    "status": "success",
                    "bucket_count": len(bucket_names),
                    "target_bucket_exists": S3_BUCKET in bucket_names,
                    "available_buckets": bucket_names
                }
                
            except Exception as e:
                logger.error(f"❌ Failed to list buckets: {str(e)}")
                raise AirflowException(f"Cannot list S3 buckets with connection '{AWS_CONN_ID}': {str(e)}")
                
        except Exception as e:
            logger.error(f"❌ AWS connection validation failed: {str(e)}")
            raise AirflowException(f"AWS connection validation failed for '{AWS_CONN_ID}': {str(e)}")

    # Data Ingestion Task Group
    with TaskGroup("data_ingestion", tooltip="Fetch and validate air quality data") as data_ingestion:
        
        @task(
            retries=2,
            retry_delay=pendulum.duration(minutes=2),
            pool="api_pool",
            pool_slots=1,
            timeout=pendulum.duration(minutes=10),
        )
        def get_air_quality_data(**kwargs) -> Dict[str, Any]:
            """
            #### Get Air Quality Data
            This task gets air quality data from the Finnish Meteorological Institute's
            open data API and prepares it for S3 upload.
            
            **Data Quality Checks:**
            - Validates API response
            - Ensures data completeness
            - Validates data types
            """
            logger = logging.getLogger(__name__)
            logical_date = kwargs["logical_date"]
            start_time = kwargs["data_interval_start"]

            try:
                params = {
                    "format": "json",
                    "precision": "double",
                    "groupareas": "0",
                    "producer": "airquality_urban",
                    "area": "Uusimaa",
                    "param": ",".join(AQ_FIELDS.keys()),
                    "starttime": start_time.isoformat(timespec="seconds"),
                    "endtime": logical_date.isoformat(timespec="seconds"),
                    "tz": "UTC",
                }

                logger.info(f"🌍 Fetching air quality data for {logical_date}")
                response = requests.get(API, params=params, timeout=30)
                response.raise_for_status()

                # Validate response
                data = response.json()
                if not data:
                    raise AirflowException("No data returned from API")
                
                logger.info(f"📊 Retrieved {len(data)} records")

                # Process data with validation
                df = pd.DataFrame(data).astype(AQ_FIELDS)
                
                # Data quality checks
                if df.empty:
                    raise AirflowException("DataFrame is empty after processing")
                
                # Check for required columns
                missing_cols = set(AQ_FIELDS.keys()) - set(df.columns)
                if missing_cols:
                    logger.warning(f"⚠️ Missing columns: {missing_cols}")
                
                # Log data summary
                logger.info(f"📈 Data shape: {df.shape}")
                logger.info(f"📅 Date range: {df['time'].min()} to {df['time'].max()}")
                
                # Convert to parquet in memory
                parquet_buffer = BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                parquet_data = parquet_buffer.getvalue()
                
                formatted_date = logical_date.format("YYYYMMDD")
                s3_key = f"{S3_PREFIX}air_quality_{formatted_date}.parquet"
                
                logger.info(f"💾 Prepared data for upload to s3://{S3_BUCKET}/{s3_key}")
                
                return {
                    "data": parquet_data,
                    "bucket": S3_BUCKET,
                    "key": s3_key,
                    "s3_path": f"s3://{S3_BUCKET}/{s3_key}",
                    "record_count": len(df),
                    "data_size_bytes": len(parquet_data),
                    "fetch_timestamp": datetime.utcnow().isoformat(),
                }
                
            except requests.RequestException as e:
                logger.error(f"❌ API request failed: {str(e)}")
                raise AirflowException(f"Failed to fetch air quality data: {str(e)}")
            except Exception as e:
                logger.error(f"❌ Unexpected error in get_air_quality_data: {str(e)}")
                raise

        @task(
            retries=1,
            retry_delay=pendulum.duration(minutes=1),
        )
        def validate_data_quality(data_info: Dict[str, Any]) -> Dict[str, Any]:
            """
            #### Validate Data Quality
            Perform comprehensive data quality validation.
            """
            logger = logging.getLogger(__name__)
            
            try:
                # Reconstruct DataFrame for validation
                df = pd.read_parquet(BytesIO(data_info["data"]))
                
                # Data quality metrics
                quality_metrics = {
                    "total_records": len(df),
                    "null_counts": df.isnull().sum().to_dict(),
                    "duplicate_records": df.duplicated().sum(),
                    "time_range_hours": (df['time'].max() - df['time'].min()).total_seconds() / 3600,
                }
                
                # Quality checks
                quality_issues = []
                
                if quality_metrics["total_records"] < 10:
                    quality_issues.append("Low record count")
                
                if quality_metrics["duplicate_records"] > 0:
                    quality_issues.append(f"Found {quality_metrics['duplicate_records']} duplicate records")
                
                # Check for missing critical data
                critical_fields = ["PM10_PT1H_avg", "PM25_PT1H_avg", "time"]
                for field in critical_fields:
                    if field in quality_metrics["null_counts"] and quality_metrics["null_counts"][field] > len(df) * 0.1:
                        quality_issues.append(f"High null rate in {field}")
                
                quality_metrics["quality_issues"] = quality_issues
                quality_metrics["quality_score"] = max(0, 100 - len(quality_issues) * 20)
                
                logger.info(f"🔍 Data Quality Score: {quality_metrics['quality_score']}/100")
                if quality_issues:
                    logger.warning(f"⚠️ Quality Issues: {', '.join(quality_issues)}")
                else:
                    logger.info("✅ Data quality validation passed")
                
                # Add quality metrics to data_info
                data_info["quality_metrics"] = quality_metrics
                return data_info
                
            except Exception as e:
                logger.error(f"❌ Data quality validation failed: {str(e)}")
                raise AirflowException(f"Data quality validation failed: {str(e)}")

        # Task dependencies within group
        raw_data = get_air_quality_data()
        validated_data = validate_data_quality(raw_data)

    # Data Storage Task Group
    with TaskGroup("data_storage", tooltip="Store data in S3 with validation") as data_storage:
        
        @task(
            retries=2,
            retry_delay=pendulum.duration(minutes=3),
            pool="s3_pool",
            pool_slots=1,
            timeout=pendulum.duration(minutes=15),
        )
        def upload_to_s3(data_info: Dict[str, Any]) -> str:
            """
            #### Upload to S3
            Upload the parquet data to S3 using S3Hook with proper error handling
            and validation.
            
            **Features:**
            - Validates data before upload
            - Checks S3 connection
            - Verifies successful upload
            """
            logger = logging.getLogger(__name__)
            
            try:
                # Validate input data
                required_keys = ["data", "bucket", "key", "s3_path"]
                for key in required_keys:
                    if key not in data_info:
                        raise AirflowException(f"Missing required key: {key}")
                
                logger.info(f"📤 Uploading {data_info.get('record_count', 'unknown')} records to S3")
                logger.info(f"📏 Data size: {data_info.get('data_size_bytes', 'unknown')} bytes")
                
                # Initialize S3 hook with configurable connection
                try:
                    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
                    logger.info(f"🔗 Using AWS connection: {AWS_CONN_ID}")
                except Exception as e:
                    logger.error(f"❌ Failed to initialize S3Hook with connection {AWS_CONN_ID}: {str(e)}")
                    raise AirflowException(f"Cannot initialize S3 connection '{AWS_CONN_ID}': {str(e)}")
                
                # Test S3 connection and bucket access
                try:
                    s3_hook.check_for_bucket(bucket_name=data_info["bucket"])
                    logger.info(f"✅ S3 bucket {data_info['bucket']} is accessible")
                except Exception as e:
                    logger.error(f"❌ S3 bucket check failed: {str(e)}")
                    raise AirflowException(f"Cannot access S3 bucket {data_info['bucket']} with connection '{AWS_CONN_ID}': {str(e)}")
                
                # Upload data
                s3_hook.load_bytes(
                    bytes_data=data_info["data"],
                    key=data_info["key"],
                    bucket_name=data_info["bucket"],
                    replace=True
                )
                
                # Verify upload
                if s3_hook.check_for_key(key=data_info["key"], bucket_name=data_info["bucket"]):
                    logger.info(f"✅ Successfully uploaded to {data_info['s3_path']}")
                else:
                    raise AirflowException("Upload verification failed - file not found in S3")
                
                return data_info["s3_path"]
                
            except Exception as e:
                logger.error(f"❌ S3 upload failed: {str(e)}")
                raise AirflowException(f"Failed to upload to S3: {str(e)}")

        # Store data
        s3_path = upload_to_s3(validated_data)

    # Data Analysis Task Group
    with TaskGroup("data_analysis", tooltip="Analyze air quality data") as data_analysis:
        
        @task(
            retries=1,
            retry_delay=pendulum.duration(minutes=2),
            pool="analysis_pool",
            pool_slots=1,
            timeout=pendulum.duration(minutes=20),
        )
        def analyze(s3_path: str, **kwargs) -> Dict[str, Any]:
            """
            #### Analyze Air Quality Data
            This task downloads the data from S3 and performs comprehensive analysis.
            
            **Analysis Features:**
            - Data quality validation
            - Statistical analysis
            - Air quality metrics calculation
            - Data completeness checks
            """
            import duckdb
            import tempfile
            import os
            
            logger = logging.getLogger(__name__)
            
            try:
                # Extract bucket and key from S3 path
                s3_path_clean = s3_path.replace("s3://", "")
                bucket, key = s3_path_clean.split("/", 1)
                
                logger.info(f"🔍 Analyzing data from {s3_path}")
                
                # Download from S3 using S3Hook with configurable connection
                try:
                    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
                    logger.info(f"🔗 Using AWS connection: {AWS_CONN_ID}")
                except Exception as e:
                    logger.error(f"❌ Failed to initialize S3Hook with connection {AWS_CONN_ID}: {str(e)}")
                    raise AirflowException(f"Cannot initialize S3 connection '{AWS_CONN_ID}': {str(e)}")
                
                # Verify file exists before download
                try:
                    if not s3_hook.check_for_key(key=key, bucket_name=bucket):
                        raise AirflowException(f"File not found in S3: {s3_path}")
                    logger.info(f"✅ File exists in S3: {s3_path}")
                except Exception as e:
                    logger.error(f"❌ File verification failed: {str(e)}")
                    raise AirflowException(f"Cannot verify file existence in S3: {str(e)}")
                
                parquet_data = s3_hook.read_key(key=key, bucket_name=bucket)
                logger.info(f"📥 Downloaded {len(parquet_data)} bytes from S3")
                
                # Analyze with DuckDB
                conn = duckdb.connect(database=":memory:")
                
                # Create a temporary file for DuckDB
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                    tmp_file.write(parquet_data)
                    tmp_file.flush()
                    
                    try:
                        # Load data into DuckDB
                        conn.execute(f"CREATE OR REPLACE TABLE airquality_urban AS SELECT * FROM read_parquet('{tmp_file.name}')")
                        
                        # Get basic statistics
                        df2 = conn.execute("SELECT * FROM airquality_urban").fetchdf()
                        
                        # Data quality checks
                        if df2.empty:
                            raise AirflowException("No data available for analysis")
                        
                        # Calculate statistics
                        stats_query = """
                        SELECT 
                            COUNT(*) as record_count,
                            MIN(time) as earliest_time,
                            MAX(time) as latest_time,
                            AVG(PM10_PT1H_avg) as avg_pm10,
                            AVG(PM25_PT1H_avg) as avg_pm25,
                            AVG(O3_PT1H_avg) as avg_o3,
                            AVG(NO2_PT1H_avg) as avg_no2,
                            AVG(CO_PT1H_avg) as avg_co,
                            AVG(SO2_PT1H_avg) as avg_so2,
                            AVG(AQINDEX_PT1H_avg) as avg_aqi
                        FROM airquality_urban
                        """
                        
                        stats = conn.execute(stats_query).fetchdf().iloc[0]
                        
                        # Air quality assessment
                        aqi_level = get_aqi_level(stats['avg_aqi'])
                        
                        # Log results
                        logger.info(f"📊 Analysis Results:")
                        logger.info(f"   📈 Record count: {stats['record_count']}")
                        logger.info(f"   📅 Time range: {stats['earliest_time']} to {stats['latest_time']}")
                        logger.info(f"   🌫️ Average PM10: {stats['avg_pm10']:.2f} μg/m³")
                        logger.info(f"   🌫️ Average PM2.5: {stats['avg_pm25']:.2f} μg/m³")
                        logger.info(f"   ☀️ Average O3: {stats['avg_o3']:.2f} μg/m³")
                        logger.info(f"   🚗 Average NO2: {stats['avg_no2']:.2f} μg/m³")
                        logger.info(f"   🎯 Air Quality Index: {stats['avg_aqi']:.1f} ({aqi_level})")
                        
                        # Data completeness check
                        completeness_query = """
                        SELECT 
                            COUNT(*) as total_records,
                            COUNT(PM10_PT1H_avg) as pm10_records,
                            COUNT(PM25_PT1H_avg) as pm25_records,
                            COUNT(O3_PT1H_avg) as o3_records
                        FROM airquality_urban
                        """
                        completeness = conn.execute(completeness_query).fetchdf().iloc[0]
                        
                        pm10_completeness = (completeness['pm10_records'] / completeness['total_records']) * 100
                        pm25_completeness = (completeness['pm25_records'] / completeness['total_records']) * 100
                        
                        logger.info(f"📈 Data Completeness:")
                        logger.info(f"   PM10: {pm10_completeness:.1f}%")
                        logger.info(f"   PM2.5: {pm25_completeness:.1f}%")
                        
                        # Return analysis results
                        analysis_results = {
                            "analysis_timestamp": datetime.utcnow().isoformat(),
                            "record_count": int(stats['record_count']),
                            "time_range": {
                                "start": str(stats['earliest_time']),
                                "end": str(stats['latest_time']),
                                "duration_hours": float((stats['latest_time'] - stats['earliest_time']).total_seconds() / 3600)
                            },
                            "air_quality": {
                                "pm10_avg": float(stats['avg_pm10']),
                                "pm25_avg": float(stats['avg_pm25']),
                                "o3_avg": float(stats['avg_o3']),
                                "no2_avg": float(stats['avg_no2']),
                                "co_avg": float(stats['avg_co']),
                                "so2_avg": float(stats['avg_so2']),
                                "aqi_avg": float(stats['avg_aqi']),
                                "aqi_level": aqi_level
                            },
                            "data_quality": {
                                "pm10_completeness": float(pm10_completeness),
                                "pm25_completeness": float(pm25_completeness),
                                "overall_completeness": float((pm10_completeness + pm25_completeness) / 2)
                            }
                        }
                        
                        logger.info("✅ Analysis completed successfully")
                        return analysis_results
                        
                    finally:
                        # Clean up temporary file
                        if os.path.exists(tmp_file.name):
                            os.unlink(tmp_file.name)
                            
            except Exception as e:
                logger.error(f"❌ Analysis failed: {str(e)}")
                raise AirflowException(f"Failed to analyze data: {str(e)}")

        # Analyze data
        analysis_results = analyze(s3_path)

    # Final task to log summary
    @task
    def log_processing_summary(analysis_results: Dict[str, Any], **kwargs):
        """
        #### Log Processing Summary
        Log a comprehensive summary of the data processing pipeline.
        """
        logger = logging.getLogger(__name__)
        
        logger.info("🎉 Air Quality Data Processing Complete!")
        logger.info(f"📊 Processed {analysis_results['record_count']} records")
        logger.info(f"⏱️ Time range: {analysis_results['time_range']['start']} to {analysis_results['time_range']['end']}")
        logger.info(f"🌍 Air Quality Index: {analysis_results['air_quality']['aqi_avg']:.1f} ({analysis_results['air_quality']['aqi_level']})")
        logger.info(f"📈 Data completeness: {analysis_results['data_quality']['overall_completeness']:.1f}%")
        
        return {
            "status": "success",
            "summary": analysis_results,
            "processing_timestamp": datetime.utcnow().isoformat()
        }

    # Task flow with proper dependencies
    connection_status = validate_aws_connection()
    summary = log_processing_summary(analysis_results)

example_objectstorage_operators()
