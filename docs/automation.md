# Automation Guide

## Pipeline Scheduling

1. Airflow DAG - Run the dockerized python code in the airflow container

## Cloud Deployment (GCP)

1. Infrastructure

- Cloud Storage for raw files : If the user uploads file to FTP, the file shoudl be moved to the raw folder in the cloud storage
- Cloud Dataflow for database
- Cloud Functions for processing
- Cloud Scheduler for orchestration

2. Data Flow

   [Cloud Storage] --> [Cloud Function] --> [Cloud Dataflow] --> [Cloud BQ] --> [Cloud Scheduler]

## Monitoring

1. Cloud Monitoring

- Custom metrics
- Alerts
- Dashboards

2. Error Recovery

- Automatic retries
- Error notifications
- Manual intervention points
