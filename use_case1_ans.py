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
# 1. Write a function evaluate_pipeline_health(log) that takes a single log dictionary and returns the same dictionary with a new
# key health_status assigned based on the above rules.

pipeline_log = {
            "pipeline_name": "user_events_ingestion",
            "status_code": 200,
            "duration_seconds": 452,
            "record_count": 124500,
            "max_latency_seconds": 5.6,
            "errors": [""],
            "warnings": ["late data arrival"],
            "ingestion_time": "2025-10-08T02:30:00Z",
            "source": "kafka"
        }

def evaluate_pipeline_health(log):
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
    return log

result = evaluate_pipeline_health(pipeline_log)
# print(result)


