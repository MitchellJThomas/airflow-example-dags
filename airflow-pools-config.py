"""
Airflow Resource Pools Configuration
This file defines resource pools for better task scheduling and resource management.
"""

# Resource Pools for S3 Object Storage DAG
POOLS_CONFIG = {
    "api_pool": {
        "description": "Pool for external API calls (rate limiting)",
        "slots": 3,
        "include_deferred": False,
    },
    "s3_pool": {
        "description": "Pool for S3 operations (bandwidth limiting)",
        "slots": 2,
        "include_deferred": False,
    },
    "analysis_pool": {
        "description": "Pool for data analysis tasks (CPU intensive)",
        "slots": 2,
        "include_deferred": False,
    },
}

# Task-specific configurations
TASK_CONFIGS = {
    "get_air_quality_data": {
        "pool": "api_pool",
        "pool_slots": 1,
        "retries": 2,
        "retry_delay": "00:02:00",
        "timeout": "00:10:00",
    },
    "upload_to_s3": {
        "pool": "s3_pool", 
        "pool_slots": 1,
        "retries": 2,
        "retry_delay": "00:03:00",
        "timeout": "00:15:00",
    },
    "analyze": {
        "pool": "analysis_pool",
        "pool_slots": 1,
        "retries": 1,
        "retry_delay": "00:02:00",
        "timeout": "00:20:00",
    },
}
