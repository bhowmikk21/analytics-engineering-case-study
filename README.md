# analytics-engineering-case-study
End-to-end data pipeline built using Snowflake, dbt, and Tableau to unify customer status reports from 7 independent source systems, normalize hierarchies, and deliver a single source of truth for reporting.

# Unified Customer Status Reporting Pipeline

This repository contains a dbt-based data pipeline that integrates point-in-time customer reports from 7 independent source systems. It standardizes metrics, resolves naming inconsistencies, and flattens multi-tier customer hierarchies to deliver a single, unified customer status mart for downstream consumption in Tableau.

## Features
- Ingests reports via Snowpipe / Python / Fivetren
- Supports 1-tier, 2-tier, and 3-tier customer hierarchies
- Normalizes customer names using a curated mapping table
- Built on Snowflake + dbt + Tableau

## Structure
- `staging/`: Raw report standardization
- `intermediate/`: Name mapping, hierarchy flattening
- `marts/`: Final unified customer status table

## Tools
- Fivetren
- Snowflake
- dbt
- Tableau
- Airflow
- GIT

<img width="1644" height="1064" alt="image" src="https://github.com/user-attachments/assets/768dfe02-0b3b-48fb-9b6e-576ff7ef3bd8" />
