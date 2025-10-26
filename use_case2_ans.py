# Scenario:
# You're building a monitoring system for a data pipeline in a data lakehouse architecture. 
# Your job is to analyze the metadata logs from multiple data sources and determine the 
# health status of each pipeline run.
# Each pipeline run is logged in a JSON-like format that will be provided below.
# You are given a list of such logs for different pipelines. Your task is to analyze each log and apply the 
# following complex conditional rules to determine the pipeline health status:

# ✅ Evaluation Rules:

# Assign a health_status field with one of the following values:
#     - "HEALTHY"
#     - "WARNING"
#     - "CRITICAL"

# Based on the following logic:
# 1. HEALTHY if:
#     - status_code is 200 AND
#     - errors is empty AND
#     - warnings is empty or only includes "late data arrival" AND
#     - duration_seconds is less than 600 AND
#     - max_latency_seconds is less than 10

# 2. WARNING if any of the following:
#     - status_code is 200 AND
#         - duration_seconds is between 600 and 1200 OR
#         - max_latency_seconds is between 10 and 30 OR
#         - warnings contains non-late data warning messages
#     - OR there are fewer than 100 records ingested (record_count < 100) but no errors

# 3. CRITICAL if:
#     - status_code is not 200
#     - OR there are one or more errors
#     - OR duration_seconds > 1200
#     - OR max_latency_seconds > 30
#     - OR record_count == 0

# 🎯 Your Task:
# 2. Write a function evaluate_all_pipelines(logs: List[Dict]) -> List[Dict] to apply this to a list of logs.

logs = [
        {
            "pipeline_name": "user_events_ingestion",
            "status_code": 200,
            "duration_seconds": 452,
            "record_count": 124500,
            "max_latency_seconds": 5.6,
            "errors": [],
            "warnings": ["late data arrival"],
            "ingestion_time": "2025-10-08T02:30:00Z",
            "source": "kafka"
        },
        {
            "pipeline_name": "transaction_data_load",
            "status_code": 500,
            "duration_seconds": 1300,
            "record_count": 0,
            "max_latency_seconds": 45.2,
            "errors": ["Database connection timeout"],
            "warnings": [],
            "ingestion_time": "2025-10-08T14:15:00Z",
            "source": "s3"
        },
        {
            "pipeline_name": "product_catalog_sync",
            "status_code": 200,
            "duration_seconds": 800,
            "record_count": 80,
            "max_latency_seconds": 15.0,
            "errors": [],
            "warnings": ["schema mismatch"],
            "ingestion_time": "2025-10-08T09:00:00Z",
            "source": "api"
        },
        {
            "pipeline_name": "inventory_update",
            "status_code": 200,
            "duration_seconds": 300,
            "record_count": 1500,
            "max_latency_seconds": 8.0,
            "errors": [],
            "warnings": [],
            "ingestion_time": "2025-10-08T03:45:00Z",
            "source": "ftp"
        }
    ]



def evaluate_all_pipelines(logs):
    pipelines = []
    for log in logs:
        if (
            log["status_code"] == 200 and
            log["errors"] == [] and
            (
                log["warnings"] == [] or
                log["warnings"] == ["late data arrival"]
            ) and
            log["duration_seconds"] < 600 and
            log["max_latency_seconds"] < 10
            ):

            log.update(health_status = "HEALTHY")
        
        elif (
            log["status_code"] == 200 and
            log["errors"] == [] and
            (
                log["duration_seconds"] > 600 and 
                log["duration_seconds"]  < 1200
            ) or 
            ( 
                log["max_latency_seconds"] > 10 and 
                log["max_latency_seconds"] < 30 
            ) or
            log["warnings"] == ["non_late data"] or 
            log["record_count"] < 100
            ):
            log.update(health_status = "WARNING")
        
        elif (
            log["status_code"] != 200 or
            log["errors"] != [] or
            log["duration_seconds"] > 1200 or
            log["max_latency_seconds"] > 30 or
            log["record_count"] == 0
            ):
        
            log.update(health_status = "CRITICAL")
    return logs
    
list_result = evaluate_all_pipelines(logs)
print(list_result)

# 🎯 Your Task:
# 3. Print a summary:

#     - Total pipelines evaluated

#     - Count of each health status category

print("Nos of logs: ", len(list_result))

import pandas as pd
df = pd.DataFrame(logs)
# print(df)
print(df["health_status"].value_counts())

    


