# Analytics Engineering Case Study
End-to-end data pipeline built using Snowflake, dbt, and Airflow to unify customer status reports from 7 independent source systems, normalize hierarchies, and deliver a single source of truth for reporting.

# Unified Customer Status Reporting Pipeline

This repository contains a dbt-based data pipeline that integrates point-in-time customer reports from 7 independent source systems. It standardizes metrics, resolves naming inconsistencies, and flattens multi-tier customer hierarchies to deliver a single, unified customer status mart for downstream consumption in Tableau.

## Features
- Ingests reports via Snowpipe / Python / Fivetren
- Supports 1-tier, 2-tier, and 3-tier customer hierarchies
- Built on Snowflake + dbt + Airflow

## Structure
- `staging/`: Raw report standardization
- `intermediate/`: Joining reports from multiple source and joining heirarchy informations
- `marts/`: Final unified customer status table

## Tools
- Fivetren
- Snowflake
- dbt
- Airflow
- GIT

## Data Pipeline:

<img width="822" height="532" alt="image" src="https://github.com/user-attachments/assets/768dfe02-0b3b-48fb-9b6e-576ff7ef3bd8" />

## Data Model:

<img width="425" height="460" alt="image" src="https://github.com/user-attachments/assets/326237a3-31d6-437d-977c-8a15d7e9caf4" />





## Requirements:
Designing a cloud architecture considering business requirements, which include components from the following layers:
- Ingestion
- Storage
- Transformation
- Scheduling
- Serving
