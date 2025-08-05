-- All credentials have been masked due to security

--sql
-- Use an admin role
USE ROLE ACCOUNTADMIN;

-- Create the `transform` role
CREATE ROLE IF NOT EXISTS transform;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Create the default warehouse if necessary
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Create the `dbt` user and assign to role
CREATE USER IF NOT EXISTS dbt
  PASSWORD='xxx'
  LOGIN_NAME='xxx'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE='transform'
  DEFAULT_NAMESPACE='SOCCER.RAW'
  COMMENT='DBT user used for data transformation';
GRANT ROLE transform to USER dbt;

-- Create our database and schemas
CREATE DATABASE IF NOT EXISTS SOCCER;
CREATE SCHEMA IF NOT EXISTS SOCCER.RAW;

-- Set up permissions to role `transform`
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE transform; 
GRANT ALL ON DATABASE SOCCER to ROLE transform;
GRANT ALL ON ALL SCHEMAS IN DATABASE SOCCER to ROLE transform;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE SOCCER to ROLE transform;
GRANT ALL ON ALL TABLES IN SCHEMA SOCCER.RAW to ROLE transform;
GRANT ALL ON FUTURE TABLES IN SCHEMA SOCCER.RAW to ROLE transform;

```

## Snowflake data import



```sql
-- Set up the defaults
USE WAREHOUSE COMPUTE_WH;
USE DATABASE soccer;
USE SCHEMA RAW;


CREATE STORAGE INTEGRATION sf_s3_intg
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'xxx' 
  STORAGE_ALLOWED_LOCATIONS = ('s3://apc-data-engineering/')
  ;

  
  
DESCRIBE INTEGRATION sf_s3_intg;
CREATE STAGE snowflake_stage
  STORAGE_INTEGRATION = sf_s3_intg
  URL = 's3://apc-data-engineering'
;

LIST @snowflake_stage;

-- Create our three tables and import the data from S3
-- Create table for provider1 raw data
CREATE OR REPLACE TABLE a (
    name STRING,
    ref STRING,
    status STRING,
    m1 INTEGER,
    m2 FLOAT,
    m3 INTEGER,
    m4 INTEGER,
    m5 INTEGER
);


                    
COPY INTO provider1_raw (name,
            ref,
            status,
            m1,
            m2,
            m3,
            m4,
            m5)
                   from 's3://apc-data-engineering/provider1 -Sheet1.csv'
                   CREDENTIALS = (AWS_KEY_ID= 'xxx' AWS_SECRET_KEY = 'xxx')
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    );
