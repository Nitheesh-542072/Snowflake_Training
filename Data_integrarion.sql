-- ============================================================================
-- SNOWFLAKE OPENFLOW: ENTERPRISE DATA INTEGRATION & GOVERNANCE FRAMEWORK
-- Portable Production Deployment Script (Deployable in ANY Snowflake Account)
-- Features:
--   1. Role-Based Access Control (RBAC) & Compute Warehouses
--   2. Medallion Schemas: BRONZE (Raw Transient), SILVER (Star Schema), PIPELINE_OPS (Control)
--   3. 4 Core Pre-Coded Tables: DIM_ACCOUNTS, DIM_PRODUCTS, FACT_ORDERS, FACT_SALES
--   4. Governance Control Plane & Staging Queues for Streamlit in Snowflake (app.py)
--   5. 10 Individual Modular Single-Task Stored Procedures
--   6. Automated Change Data Capture (CDC) Stream & Task
-- ============================================================================

-- ============================================================================
-- 1. SECURITY & ACCESS CONTROL (Roles & Compute)
-- ============================================================================
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS ROLE_DATA_ENGINEER
    COMMENT = 'Data Engineering role for managing ingestion, pipelines, and ETL';

CREATE ROLE IF NOT EXISTS ROLE_ANALYST
    COMMENT = 'Business Analyst role for querying Silver dimension and fact tables';

GRANT ROLE ROLE_DATA_ENGINEER TO ROLE SYSADMIN;
GRANT ROLE ROLE_ANALYST TO ROLE SYSADMIN;

-- Dynamically grant to current user executing deployment
SET current_deployment_user = CURRENT_USER();
GRANT ROLE ROLE_DATA_ENGINEER TO USER IDENTIFIER($current_deployment_user);
GRANT ROLE ROLE_ANALYST TO USER IDENTIFIER($current_deployment_user);

USE ROLE ACCOUNTADMIN;

-- Resource Monitor for FinOps Cost Protection
CREATE OR REPLACE RESOURCE MONITOR RM_PROD
    WITH CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS 
        ON 80 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

GRANT MODIFY, MONITOR ON RESOURCE MONITOR RM_PROD TO ROLE SYSADMIN;
GRANT MONITOR ON RESOURCE MONITOR RM_PROD TO ROLE ROLE_DATA_ENGINEER;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE ROLE_DATA_ENGINEER;

-- Compute Warehouses
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS WH_DEV
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Development warehouse for data ingestion and ad-hoc engineering';

CREATE WAREHOUSE IF NOT EXISTS WH_PROD
    WAREHOUSE_SIZE = 'XSMALL'
    RESOURCE_MONITOR = RM_PROD
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Production warehouse for automated CDC tasks and ingestion procedures';

CREATE WAREHOUSE IF NOT EXISTS WH_TEST
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Testing and analyst query warehouse';

USE WAREHOUSE WH_DEV;

-- ============================================================================
-- 2. DATABASE & SCHEMAS (Medallion Architecture)
-- ============================================================================
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS RETAIL_ENTERPRISE_DB;
USE DATABASE RETAIL_ENTERPRISE_DB;

CREATE SCHEMA IF NOT EXISTS BRONZE;       -- Raw transient landing & CDC stream
CREATE SCHEMA IF NOT EXISTS SILVER;       -- Cleansed, typed, Star Schema business models
CREATE SCHEMA IF NOT EXISTS PIPELINE_OPS; -- Metadata control tables, queues & audit

-- ============================================================================
-- 3. BRONZE LAYER (Landing Stage, File Format & CDC Stream)
-- ============================================================================
USE SCHEMA BRONZE;

-- Pipe-delimited CSV format for JSON payload ingress
CREATE OR REPLACE FILE FORMAT BRONZE.FF_CSV_PIPE
    TYPE = 'CSV'
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE'
    NULL_IF = ('NULL', '')
    TRIM_SPACE = TRUE
    COMMENT = 'Pipe-delimited CSV format for raw JSON payload ingress';

-- Named internal landing stage for batch files
CREATE OR REPLACE STAGE BRONZE.STG_OPENFLOW_INGRESS
    DIRECTORY = (ENABLE = TRUE)
    FILE_FORMAT = BRONZE.FF_CSV_PIPE
    COMMENT = 'Internal landing stage for raw batch CSV files';

-- Transient Bronze table (auto-purges after 24 hrs to eliminate Fail-Safe costs)
CREATE OR REPLACE TRANSIENT TABLE BRONZE.OPENFLOW_RAW_STREAM (
    PAYLOAD_TYPE VARCHAR(50),
    RAW_RECORD VARIANT,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) DATA_RETENTION_TIME_IN_DAYS = 1;

-- Change Data Capture (CDC) Stream tracking unprocessed delta rows
CREATE OR REPLACE STREAM BRONZE.STR_OPENFLOW_STREAM 
ON TABLE BRONZE.OPENFLOW_RAW_STREAM;

-- ============================================================================
-- 4. PIPELINE OPERATIONS (Metadata & Governance Control Plane)
-- ============================================================================
USE SCHEMA PIPELINE_OPS;

-- High-watermark tracker for incremental ingestion
CREATE OR REPLACE TABLE PIPELINE_OPS.WATERMARKS (
    PIPELINE_NAME VARCHAR(50) PRIMARY KEY,
    LAST_WATERMARK TIMESTAMP_NTZ
);

-- Dead Letter Queue (DLQ) for quarantined rogue records
CREATE OR REPLACE TABLE PIPELINE_OPS.DEAD_LETTER_QUEUE (
    ERROR_ID INT AUTOINCREMENT PRIMARY KEY,
    PIPELINE_NAME VARCHAR(50),
    RAW_PAYLOAD VARIANT,
    ERROR_MSG VARCHAR(500),
    RETRY_COUNT INT DEFAULT 0,
    STATUS VARCHAR(20) DEFAULT 'QUEUED',
    LOGGED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Source Ingestion Control (Central Metadata Catalog for Pipelines)
CREATE OR REPLACE TABLE PIPELINE_OPS.SOURCE_INGESTION_CONTROL (
    SOURCE_ID INT AUTOINCREMENT PRIMARY KEY,
    SOURCE_NAME VARCHAR(100),
    PAYLOAD_TYPE VARCHAR(50) UNIQUE,
    TARGET_SCHEMA VARCHAR(50) DEFAULT 'SILVER',
    TARGET_TABLE VARCHAR(50),
    BUSINESS_KEY VARCHAR(50),
    TIE_BREAKER_COL VARCHAR(50) DEFAULT 'UPDATED_AT',
    SCHEMA_DRIFT_ACTION VARCHAR(20) DEFAULT 'ADD', -- 'ADD' or 'ALERT'
    AUTO_CREATE_TABLE BOOLEAN DEFAULT FALSE,
    APPROVAL_STATUS VARCHAR(30) DEFAULT 'APPROVED', -- 'APPROVED' or 'PENDING_REVIEW'
    FLATTEN_POLICY VARCHAR(50) DEFAULT 'LATEST_ONLY', -- 'LATEST_ONLY' or 'ALL_ELEMENTS'
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Seed the 4 Pre-Coded Core Pipelines: ACCOUNT, PRODUCT, ORDER, SALE
MERGE INTO PIPELINE_OPS.SOURCE_INGESTION_CONTROL tgt
USING (
    SELECT 'SALESFORCE_CRM' AS S_NAME, 'ACCOUNT'  AS P_TYPE, 'SILVER' AS T_SCH, 'DIM_ACCOUNTS'  AS T_TBL, 'ACCOUNT_ID'  AS B_KEY, 'UPDATED_AT' AS TB_COL, 'ADD'   AS D_ACT, FALSE AS A_CRT, 'APPROVED' AS APP_STAT UNION ALL
    SELECT 'ERP_CATALOG'    AS S_NAME, 'PRODUCT'  AS P_TYPE, 'SILVER' AS T_SCH, 'DIM_PRODUCTS'  AS T_TBL, 'PRODUCT_ID'  AS B_KEY, 'UPDATED_AT' AS TB_COL, 'ADD'   AS D_ACT, FALSE AS A_CRT, 'APPROVED' AS APP_STAT UNION ALL
    SELECT 'ORDER_SERVICE'  AS S_NAME, 'ORDER'    AS P_TYPE, 'SILVER' AS T_SCH, 'FACT_ORDERS'   AS T_TBL, 'ORDER_ID'    AS B_KEY, 'UPDATED_AT' AS TB_COL, 'ALERT' AS D_ACT, FALSE AS A_CRT, 'APPROVED' AS APP_STAT UNION ALL
    SELECT 'POS_CHECKOUT'   AS S_NAME, 'SALE'     AS P_TYPE, 'SILVER' AS T_SCH, 'FACT_SALES'    AS T_TBL, 'SALE_ID'     AS B_KEY, 'SALE_TIME'  AS TB_COL, 'ALERT' AS D_ACT, FALSE AS A_CRT, 'APPROVED' AS APP_STAT
) src ON tgt.PAYLOAD_TYPE = src.P_TYPE
WHEN MATCHED THEN 
    UPDATE SET tgt.SOURCE_NAME = src.S_NAME, tgt.TARGET_SCHEMA = src.T_SCH, tgt.TARGET_TABLE = src.T_TBL, tgt.BUSINESS_KEY = src.B_KEY, tgt.TIE_BREAKER_COL = src.TB_COL, tgt.SCHEMA_DRIFT_ACTION = src.D_ACT, tgt.APPROVAL_STATUS = src.APP_STAT
WHEN NOT MATCHED THEN 
    INSERT (SOURCE_NAME, PAYLOAD_TYPE, TARGET_SCHEMA, TARGET_TABLE, BUSINESS_KEY, TIE_BREAKER_COL, SCHEMA_DRIFT_ACTION, AUTO_CREATE_TABLE, APPROVAL_STATUS)
    VALUES (src.S_NAME, src.P_TYPE, src.T_SCH, src.T_TBL, src.B_KEY, src.TB_COL, src.D_ACT, src.A_CRT, src.APP_STAT);

-- Schema Evolution Policy Table
CREATE OR REPLACE TABLE PIPELINE_OPS.SCHEMA_EVOLUTION_CONTROL (
    PIPELINE_NAME VARCHAR(50) PRIMARY KEY,
    TARGET_SCHEMA VARCHAR(50) DEFAULT 'SILVER',
    TARGET_TABLE VARCHAR(50),
    SCHEMA_DRIFT_ACTION VARCHAR(20) DEFAULT 'ADD',
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

MERGE INTO PIPELINE_OPS.SCHEMA_EVOLUTION_CONTROL tgt
USING (
    SELECT 'ACCOUNT' AS P, 'SILVER' AS S, 'DIM_ACCOUNTS' AS T, 'ADD'   AS A UNION ALL
    SELECT 'PRODUCT' AS P, 'SILVER' AS S, 'DIM_PRODUCTS' AS T, 'ADD'   AS A UNION ALL
    SELECT 'ORDER'   AS P, 'SILVER' AS S, 'FACT_ORDERS'  AS T, 'ALERT' AS A UNION ALL
    SELECT 'SALE'    AS P, 'SILVER' AS S, 'FACT_SALES'   AS T, 'ALERT' AS A
) src ON tgt.PIPELINE_NAME = src.P
WHEN MATCHED THEN UPDATE SET tgt.TARGET_SCHEMA = src.S, tgt.TARGET_TABLE = src.T, tgt.SCHEMA_DRIFT_ACTION = src.A
WHEN NOT MATCHED THEN INSERT (PIPELINE_NAME, TARGET_SCHEMA, TARGET_TABLE, SCHEMA_DRIFT_ACTION) VALUES (src.P, src.S, src.T, src.A);

-- Schema Drift Audit Log: Tracks all drift approvals and rejections
CREATE OR REPLACE TABLE PIPELINE_OPS.SCHEMA_DRIFT_LOG (
    LOG_ID INT AUTOINCREMENT PRIMARY KEY,
    PIPELINE_NAME VARCHAR(50),
    TARGET_TABLE VARCHAR(50),
    DETECTED_COLUMN VARCHAR(100),
    DRIFT_ACTION VARCHAR(20),
    EXECUTION_STATUS VARCHAR(30),
    MESSAGE VARCHAR(500),
    LOGGED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Governance Review Staging Tables for Streamlit App (app.py)
CREATE TABLE IF NOT EXISTS PIPELINE_OPS.PENDING_NEW_TABLE_COLUMNS (
    DISCOVERY_ID INT AUTOINCREMENT PRIMARY KEY,
    PAYLOAD_TYPE VARCHAR(50),
    SUGGESTED_TABLE_NAME VARCHAR(50),
    COLUMN_NAME VARCHAR(100),
    INFERRED_DATA_TYPE VARCHAR(50),
    IS_PRIMARY_KEY_CANDIDATE BOOLEAN DEFAULT FALSE,
    IS_SELECTED BOOLEAN DEFAULT TRUE,
    SAMPLE_VALUE VARCHAR(500),
    DISCOVERED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    STATUS VARCHAR(30) DEFAULT 'PENDING_REVIEW'
);

CREATE TABLE IF NOT EXISTS PIPELINE_OPS.SCHEMA_DRIFT_APPROVAL_QUEUE (
    QUEUE_ID INT AUTOINCREMENT PRIMARY KEY,
    PAYLOAD_TYPE VARCHAR(50),
    TARGET_SCHEMA VARCHAR(50) DEFAULT 'SILVER',
    TARGET_TABLE VARCHAR(50),
    DETECTED_COLUMN VARCHAR(100),
    INFERRED_DATA_TYPE VARCHAR(50),
    SAMPLE_VALUE VARCHAR(500),
    DEFAULT_POLICY_ACTION VARCHAR(20),
    STATUS VARCHAR(30) DEFAULT 'PENDING_REVIEW',
    REVIEWED_BY VARCHAR(100),
    REVIEWED_AT TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Pipeline Metrics Observability Table
CREATE OR REPLACE TABLE PIPELINE_OPS.PIPELINE_METRICS (
    PIPELINE_NAME VARCHAR(50),
    SOURCE VARCHAR(50),
    LAST_RUN TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SUCCESS_RECORDS INT DEFAULT 0,
    FAILED_RECORDS INT DEFAULT 0,
    THROUGHPUT_REC_SEC NUMBER(10,2) DEFAULT 0,
    FLATTENING_MODE VARCHAR(50) DEFAULT 'DIRECT'
);

-- Monitoring Dashboard View
CREATE OR REPLACE VIEW PIPELINE_OPS.V_MONITORING_DASHBOARD AS
SELECT 
    PIPELINE_NAME      AS "Pipeline Name",
    SOURCE             AS "Source",
    LAST_RUN           AS "Last Run",
    ROUND(
        IFF((SUCCESS_RECORDS + FAILED_RECORDS) > 0, 
            (SUCCESS_RECORDS / (SUCCESS_RECORDS + FAILED_RECORDS)) * 100, 
            100.00), 2
    )                  AS "Success %",
    FAILED_RECORDS     AS "Failed Records",
    THROUGHPUT_REC_SEC AS "Throughput",
    FLATTENING_MODE    AS "Flatten Mode",
    CASE 
        WHEN FAILED_RECORDS = 0 THEN '🟢 HEALTHY'
        WHEN (SUCCESS_RECORDS / NULLIF(SUCCESS_RECORDS + FAILED_RECORDS, 0)) >= 0.80 THEN '🟡 WARNING'
        ELSE '🔴 CRITICAL'
    END                AS "Health Status"
FROM PIPELINE_OPS.PIPELINE_METRICS;

-- ============================================================================
-- 5. SILVER LAYER: 4 PRE-DEFINED CORE STAR SCHEMA TABLES
-- If you dropped the SILVER schema, running this section recreates the schema
-- and immediately provisions the 4 pre-defined core tables.
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS SILVER;
USE SCHEMA SILVER;

-- [CORE TABLE 1] Customer Accounts Dimension
CREATE OR REPLACE TABLE SILVER.DIM_ACCOUNTS (
    ACCOUNT_ID VARCHAR(50),
    ACCOUNT_NAME VARCHAR(150),
    TIER VARCHAR(20),
    IS_DELETED BOOLEAN DEFAULT FALSE,
    UPDATED_AT TIMESTAMP_NTZ,
    ETL_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DIM_ACCOUNTS PRIMARY KEY (ACCOUNT_ID)
) 
COMMENT = 'DIMENSION TABLE: Customer Account Profiles'
DATA_RETENTION_TIME_IN_DAYS = 7;

-- [CORE TABLE 2] Products Catalog Dimension
CREATE OR REPLACE TABLE SILVER.DIM_PRODUCTS (
    PRODUCT_ID VARCHAR(50),
    PRODUCT_NAME VARCHAR(150),
    CATEGORY VARCHAR(50),
    UNIT_PRICE NUMBER(10,2),
    UPDATED_AT TIMESTAMP_NTZ,
    ETL_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DIM_PRODUCTS PRIMARY KEY (PRODUCT_ID)
) 
COMMENT = 'DIMENSION TABLE: Product Catalog Master'
DATA_RETENTION_TIME_IN_DAYS = 7;

-- [CORE TABLE 3] Orders Header Fact
CREATE OR REPLACE TABLE SILVER.FACT_ORDERS (
    ORDER_ID INT,
    CUSTOMER_ID VARCHAR(50),
    ORDER_STATUS VARCHAR(30),
    ORDER_TIMESTAMP TIMESTAMP_NTZ,
    ETL_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_FACT_ORDERS PRIMARY KEY (ORDER_ID),
    CONSTRAINT FK_FACT_ORDERS_CUSTOMER FOREIGN KEY (CUSTOMER_ID) REFERENCES SILVER.DIM_ACCOUNTS(ACCOUNT_ID)
) 
COMMENT = 'FACT TABLE: Order Headers'
DATA_RETENTION_TIME_IN_DAYS = 7;

-- [CORE TABLE 4] Sales Line Items Fact
CREATE OR REPLACE TABLE SILVER.FACT_SALES (
    SALE_ID INT,
    ORDER_ID INT,
    PRODUCT_ID VARCHAR(50),
    QUANTITY INT,
    UNIT_PRICE NUMBER(10,2),
    DISCOUNT_AMOUNT NUMBER(10,2),
    NET_REVENUE NUMBER(12,2),
    PAYMENT_METHOD VARCHAR(30),
    SALE_TIMESTAMP TIMESTAMP_NTZ,
    ETL_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_FACT_SALES PRIMARY KEY (SALE_ID),
    CONSTRAINT FK_FACT_SALES_ORDER FOREIGN KEY (ORDER_ID) REFERENCES SILVER.FACT_ORDERS(ORDER_ID),
    CONSTRAINT FK_FACT_SALES_PRODUCT FOREIGN KEY (PRODUCT_ID) REFERENCES SILVER.DIM_PRODUCTS(PRODUCT_ID)
) 
COMMENT = 'FACT TABLE: Sales Line Items Grain'
DATA_RETENTION_TIME_IN_DAYS = 7;

-- Data Masking Policy for Customer Privacy
CREATE OR REPLACE MASKING POLICY SILVER.MASK_ACCOUNT_NAME AS (val VARCHAR) RETURNS VARCHAR ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ROLE_DATA_ENGINEER') THEN val
        ELSE LEFT(val, 2) || '**** (Masked)'
    END;

ALTER TABLE SILVER.DIM_ACCOUNTS MODIFY COLUMN ACCOUNT_NAME SET MASKING POLICY SILVER.MASK_ACCOUNT_NAME;

-- Backward-Compatible Views (Supports legacy queries referencing original table names)
CREATE OR REPLACE VIEW SILVER.ACCOUNTS AS SELECT * FROM SILVER.DIM_ACCOUNTS;
CREATE OR REPLACE VIEW SILVER.PRODUCTS AS SELECT * FROM SILVER.DIM_PRODUCTS;
CREATE OR REPLACE VIEW SILVER.ORDERS   AS SELECT * FROM SILVER.FACT_ORDERS;
CREATE OR REPLACE VIEW SILVER.SALES    AS SELECT * FROM SILVER.FACT_SALES;

-- ============================================================================
-- 6. MODULAR SINGLE-TASK STORED PROCEDURES (10 Individual Tasks)
-- ============================================================================
USE SCHEMA PIPELINE_OPS;

-- PROCEDURE 1: DETECT PENDING APPROVALS
CREATE OR REPLACE PROCEDURE PIPELINE_OPS.SP_DETECT_PENDING_APPROVALS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_new_sources_cnt INT DEFAULT 0;
    v_drift_candidates_cnt INT DEFAULT 0;
BEGIN
    USE WAREHOUSE WH_DEV;

    -- Detect new entities not registered in control catalog
    INSERT INTO PIPELINE_OPS.PENDING_NEW_TABLE_COLUMNS 
    (PAYLOAD_TYPE, SUGGESTED_TABLE_NAME, COLUMN_NAME, INFERRED_DATA_TYPE, IS_PRIMARY_KEY_CANDIDATE, IS_SELECTED, SAMPLE_VALUE)
    WITH raw_incoming AS (
        SELECT b.PAYLOAD_TYPE, b.RAW_RECORD
        FROM BRONZE.OPENFLOW_RAW_STREAM b
        WHERE NOT EXISTS (
            SELECT 1 FROM PIPELINE_OPS.SOURCE_INGESTION_CONTROL c 
            WHERE c.PAYLOAD_TYPE = b.PAYLOAD_TYPE
        )
    ),
    flattened_fields AS (
        SELECT 
            r.PAYLOAD_TYPE,
            f.key AS COL_NAME,
            f.value AS COL_VAL,
            CASE 
                WHEN IS_INTEGER(f.value) THEN 'NUMBER(38,0)'
                WHEN IS_DECIMAL(f.value) THEN 'NUMBER(12,2)'
                WHEN IS_BOOLEAN(f.value) THEN 'BOOLEAN'
                WHEN IS_TIMESTAMP_NTZ(f.value::VARCHAR) THEN 'TIMESTAMP_NTZ'
                WHEN IS_ARRAY(f.value)   THEN 'ARRAY'
                WHEN IS_OBJECT(f.value)  THEN 'OBJECT'
                ELSE 'VARCHAR(150)'
            END AS INFERRED_TYPE,
            IFF(UPPER(f.key) LIKE '%_ID' OR UPPER(f.key) = 'ID', TRUE, FALSE) AS IS_PK
        FROM raw_incoming r,
        LATERAL FLATTEN(input => r.RAW_RECORD) f
    ),
    deduped_fields AS (
        SELECT 
            PAYLOAD_TYPE,
            PAYLOAD_TYPE AS SUGGESTED_TABLE,
            COL_NAME,
            INFERRED_TYPE,
            MAX(IS_PK) AS IS_PK,
            MAX(LEFT(COL_VAL::VARCHAR, 250)) AS SAMPLE_VAL
        FROM flattened_fields
        GROUP BY PAYLOAD_TYPE, COL_NAME, INFERRED_TYPE
    )
    SELECT d.PAYLOAD_TYPE, d.SUGGESTED_TABLE, d.COL_NAME, d.INFERRED_TYPE, d.IS_PK, TRUE AS IS_SELECTED, d.SAMPLE_VAL
    FROM deduped_fields d
    WHERE NOT EXISTS (
        SELECT 1 FROM PIPELINE_OPS.PENDING_NEW_TABLE_COLUMNS p 
        WHERE p.PAYLOAD_TYPE = d.PAYLOAD_TYPE 
          AND p.COLUMN_NAME = d.COL_NAME
          AND p.STATUS = 'PENDING_REVIEW'
    );

    SELECT COUNT(*) INTO :v_new_sources_cnt 
    FROM PIPELINE_OPS.PENDING_NEW_TABLE_COLUMNS 
    WHERE STATUS = 'PENDING_REVIEW';

    -- Detect schema drift on existing core tables
    INSERT INTO PIPELINE_OPS.SCHEMA_DRIFT_APPROVAL_QUEUE
    (PAYLOAD_TYPE, TARGET_SCHEMA, TARGET_TABLE, DETECTED_COLUMN, INFERRED_DATA_TYPE, SAMPLE_VALUE, DEFAULT_POLICY_ACTION)
    WITH incoming_records AS (
        SELECT 
            b.PAYLOAD_TYPE,
            c.TARGET_SCHEMA,
            c.TARGET_TABLE,
            COALESCE(e.SCHEMA_DRIFT_ACTION, c.SCHEMA_DRIFT_ACTION, 'ADD') AS DEFAULT_POLICY,
            b.RAW_RECORD
        FROM BRONZE.OPENFLOW_RAW_STREAM b
        JOIN PIPELINE_OPS.SOURCE_INGESTION_CONTROL c ON b.PAYLOAD_TYPE = c.PAYLOAD_TYPE
        LEFT JOIN PIPELINE_OPS.SCHEMA_EVOLUTION_CONTROL e ON b.PAYLOAD_TYPE = e.PIPELINE_NAME
        WHERE c.IS_ACTIVE = TRUE
    ),
    flattened_cols AS (
        SELECT 
            r.PAYLOAD_TYPE,
            r.TARGET_SCHEMA,
            r.TARGET_TABLE,
            r.DEFAULT_POLICY,
            UPPER(f.key) AS INCOMING_COL,
            CASE 
                WHEN IS_INTEGER(f.value) THEN 'NUMBER(38,0)'
                WHEN IS_DECIMAL(f.value) THEN 'NUMBER(12,2)'
                WHEN IS_BOOLEAN(f.value) THEN 'BOOLEAN'
                WHEN IS_TIMESTAMP_NTZ(f.value::VARCHAR) THEN 'TIMESTAMP_NTZ'
                ELSE 'VARCHAR(150)'
            END AS INFERRED_DATA_TYPE,
            LEFT(f.value::VARCHAR, 250) AS SAMPLE_VAL
        FROM incoming_records r,
        LATERAL FLATTEN(input => r.RAW_RECORD) f
        WHERE NOT IS_ARRAY(f.value)
    ),
    drift_candidates AS (
        SELECT 
            fc.PAYLOAD_TYPE, fc.TARGET_SCHEMA, fc.TARGET_TABLE, fc.DEFAULT_POLICY, fc.INCOMING_COL, fc.INFERRED_DATA_TYPE,
            MAX(fc.SAMPLE_VAL) AS SAMPLE_VAL
        FROM flattened_cols fc
        WHERE NOT EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS isc
            WHERE isc.TABLE_SCHEMA = fc.TARGET_SCHEMA
              AND isc.TABLE_NAME = fc.TARGET_TABLE
              AND isc.COLUMN_NAME = fc.INCOMING_COL
        )
        GROUP BY fc.PAYLOAD_TYPE, fc.TARGET_SCHEMA, fc.TARGET_TABLE, fc.DEFAULT_POLICY, fc.INCOMING_COL, fc.INFERRED_DATA_TYPE
    )
    SELECT dc.PAYLOAD_TYPE, dc.TARGET_SCHEMA, dc.TARGET_TABLE, dc.INCOMING_COL, dc.INFERRED_DATA_TYPE, dc.SAMPLE_VAL, dc.DEFAULT_POLICY
    FROM drift_candidates dc
    WHERE NOT EXISTS (
        SELECT 1 FROM PIPELINE_OPS.SCHEMA_DRIFT_APPROVAL_QUEUE q
        WHERE q.TARGET_TABLE = dc.TARGET_TABLE 
          AND q.DETECTED_COLUMN = dc.INCOMING_COL
          AND q.STATUS = 'PENDING_REVIEW'
    );

    SELECT COUNT(*) INTO :v_drift_candidates_cnt 
    FROM PIPELINE_OPS.SCHEMA_DRIFT_APPROVAL_QUEUE 
    WHERE STATUS = 'PENDING_REVIEW';

    RETURN 'Detection Complete: ' || v_new_sources_cnt || ' new table columns pending, ' || v_drift_candidates_cnt || ' drift columns pending review.';
END;
$$;

-- PROCEDURE 2: CREATE CUSTOM ONBOARDED TABLE
CREATE OR REPLACE PROCEDURE PIPELINE_OPS.SP_CREATE_CUSTOM_ONBOARDED_TABLE(
    p_payload_type VARCHAR,
    p_target_table VARCHAR,
    p_pk_col VARCHAR,
    p_columns_json VARCHAR
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_col_defs VARCHAR(4000) DEFAULT '';
    v_create_sql VARCHAR(4000);
    v_tbl_exists INT DEFAULT 0;
BEGIN
    USE WAREHOUSE WH_DEV;

    SELECT COUNT(*) INTO :v_tbl_exists
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'SILVER' AND TABLE_NAME = UPPER(:p_target_table);

    IF (v_tbl_exists = 0) THEN
        SELECT LISTAGG(
            '        ' || UPPER(f.value:COLUMN_NAME::VARCHAR) || ' ' || UPPER(f.value:DATA_TYPE::VARCHAR) || 
            IFF(UPPER(f.value:COLUMN_NAME::VARCHAR) = UPPER(:p_pk_col), ' PRIMARY KEY', ''),
            ',\n'
        ) WITHIN GROUP (ORDER BY f.index)
        INTO :v_col_defs
        FROM TABLE(FLATTEN(input => PARSE_JSON(:p_columns_json))) f;

        v_col_defs := v_col_defs || ',\n        ETL_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()';

        v_create_sql := 'CREATE TABLE IF NOT EXISTS SILVER.' || UPPER(:p_target_table) || ' (\n' ||
                        v_col_defs || '\n' ||
                        ') COMMENT = ''CUSTOM ONBOARDED TABLE FROM STREAMLIT UI'' DATA_RETENTION_TIME_IN_DAYS = 7;';

        EXECUTE IMMEDIATE :v_create_sql;
    END IF;

    MERGE INTO PIPELINE_OPS.SOURCE_INGESTION_CONTROL tgt
    USING (
        SELECT 
            'DYNAMIC_UI_' || UPPER(:p_payload_type) AS S_NAME,
            UPPER(:p_payload_type) AS P_TYPE,
            'SILVER' AS T_SCH,
            UPPER(:p_target_table) AS T_TBL,
            UPPER(:p_pk_col) AS B_KEY,
            'UPDATED_AT' AS TB_COL,
            'ADD' AS D_ACT,
            FALSE AS A_CRT,
            'APPROVED' AS APP_STATUS,
            TRUE AS ACT
    ) src ON tgt.PAYLOAD_TYPE = src.P_TYPE
    WHEN MATCHED THEN
        UPDATE SET tgt.TARGET_TABLE = src.T_TBL, tgt.BUSINESS_KEY = src.B_KEY, tgt.APPROVAL_STATUS = src.APP_STATUS, tgt.IS_ACTIVE = src.ACT
    WHEN NOT MATCHED THEN
        INSERT (SOURCE_NAME, PAYLOAD_TYPE, TARGET_SCHEMA, TARGET_TABLE, BUSINESS_KEY, TIE_BREAKER_COL, SCHEMA_DRIFT_ACTION, AUTO_CREATE_TABLE, APPROVAL_STATUS, IS_ACTIVE)
        VALUES (src.S_NAME, src.P_TYPE, src.T_SCH, src.T_TBL, src.B_KEY, src.TB_COL, src.D_ACT, src.A_CRT, src.APP_STATUS, src.ACT);

    UPDATE PIPELINE_OPS.PENDING_NEW_TABLE_COLUMNS
    SET STATUS = 'APPROVED'
    WHERE PAYLOAD_TYPE = UPPER(:p_payload_type);

    RETURN 'Successfully created table SILVER.' || UPPER(:p_target_table) || ' and registered in control plane.';
END;
$$;

-- PROCEDURE 2B: REJECT NEW TABLE CREATION TO DLQ
CREATE OR REPLACE PROCEDURE PIPELINE_OPS.SP_REJECT_NEW_TABLE_TO_DLQ(
    p_payload_type VARCHAR,
    p_user VARCHAR DEFAULT 'STREAMLIT_USER'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_cnt INT DEFAULT 0;
BEGIN
    USE WAREHOUSE WH_DEV;

    -- Route all records for this rejected entity to DLQ
    INSERT INTO PIPELINE_OPS.DEAD_LETTER_QUEUE (PIPELINE_NAME, RAW_PAYLOAD, ERROR_MSG, STATUS)
    SELECT 
        b.PAYLOAD_TYPE,
        b.RAW_RECORD,
        'Quarantined by Governance UI: New table creation for ' || :p_payload_type || ' was rejected.',
        'QUEUED'
    FROM BRONZE.OPENFLOW_RAW_STREAM b
    WHERE b.PAYLOAD_TYPE = UPPER(:p_payload_type);

    v_cnt := SQLROWCOUNT;

    -- Update staging columns to REJECTED
    UPDATE PIPELINE_OPS.PENDING_NEW_TABLE_COLUMNS
    SET STATUS = 'REJECTED'
    WHERE PAYLOAD_TYPE = UPPER(:p_payload_type);

    -- Log audit in SCHEMA_DRIFT_LOG
    INSERT INTO PIPELINE_OPS.SCHEMA_DRIFT_LOG 
    (PIPELINE_NAME, TARGET_TABLE, DETECTED_COLUMN, DRIFT_ACTION, EXECUTION_STATUS, MESSAGE)
    VALUES (UPPER(:p_payload_type), UPPER(:p_payload_type), 'NEW_TABLE', 'REJECT_UI', 'QUARANTINED_TO_DLQ', 
            'Rejected table creation via UI by ' || :p_user || '; ' || :v_cnt || ' record(s) sent to DLQ.');

    RETURN 'Rejected: Table creation for ' || UPPER(:p_payload_type) || ' rejected. ' || v_cnt || ' records quarantined to DLQ.';
END;
$$;

-- PROCEDURE 3: APPROVE SCHEMA DRIFT
CREATE OR REPLACE PROCEDURE PIPELINE_OPS.SP_APPROVE_SCHEMA_DRIFT(
    p_queue_id INT,
    p_user VARCHAR DEFAULT 'STREAMLIT_USER'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_target_schema VARCHAR(50);
    v_target_table VARCHAR(50);
    v_column_name VARCHAR(100);
    v_data_type VARCHAR(50);
    v_alter_sql VARCHAR(1000);
BEGIN
    USE WAREHOUSE WH_DEV;

    SELECT TARGET_SCHEMA, TARGET_TABLE, DETECTED_COLUMN, INFERRED_DATA_TYPE
    INTO :v_target_schema, :v_target_table, :v_column_name, :v_data_type
    FROM PIPELINE_OPS.SCHEMA_DRIFT_APPROVAL_QUEUE
    WHERE QUEUE_ID = :p_queue_id;

    v_alter_sql := 'ALTER TABLE ' || v_target_schema || '.' || v_target_table || 
                   ' ADD COLUMN IF NOT EXISTS ' || v_column_name || ' ' || v_data_type || ';';
    EXECUTE IMMEDIATE :v_alter_sql;

    UPDATE PIPELINE_OPS.SCHEMA_DRIFT_APPROVAL_QUEUE
    SET STATUS = 'APPROVED',
        REVIEWED_BY = :p_user,
        REVIEWED_AT = CURRENT_TIMESTAMP()
    WHERE QUEUE_ID = :p_queue_id;

    INSERT INTO PIPELINE_OPS.SCHEMA_DRIFT_LOG 
    (PIPELINE_NAME, TARGET_TABLE, DETECTED_COLUMN, DRIFT_ACTION, EXECUTION_STATUS, MESSAGE)
    VALUES (:v_target_table, :v_target_table, :v_column_name, 'APPROVE_UI', 'ALTER_SUCCESS', 'Approved via UI by ' || :p_user);

    RETURN 'Approved: Added column ' || v_column_name || ' (' || v_data_type || ') to ' || v_target_table;
END;
$$;

-- PROCEDURE 4: REJECT SCHEMA DRIFT TO DLQ
CREATE OR REPLACE PROCEDURE PIPELINE_OPS.SP_REJECT_SCHEMA_DRIFT_TO_DLQ(
    p_queue_id INT,
    p_user VARCHAR DEFAULT 'STREAMLIT_USER'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_target_table VARCHAR(50);
    v_column_name VARCHAR(100);
BEGIN
    USE WAREHOUSE WH_DEV;

    SELECT TARGET_TABLE, DETECTED_COLUMN
    INTO :v_target_table, :v_column_name
    FROM PIPELINE_OPS.SCHEMA_DRIFT_APPROVAL_QUEUE
    WHERE QUEUE_ID = :p_queue_id;

    INSERT INTO PIPELINE_OPS.DEAD_LETTER_QUEUE (PIPELINE_NAME, RAW_PAYLOAD, ERROR_MSG, STATUS)
    SELECT 
        b.PAYLOAD_TYPE,
        b.RAW_RECORD,
        'Quarantined by Governance UI: Unauthorized drift column ' || :v_column_name || ' rejected.',
        'QUEUED'
    FROM BRONZE.OPENFLOW_RAW_STREAM b
    WHERE b.RAW_RECORD[:v_column_name] IS NOT NULL;

    UPDATE PIPELINE_OPS.SCHEMA_DRIFT_APPROVAL_QUEUE
    SET STATUS = 'REJECTED',
        REVIEWED_BY = :p_user,
        REVIEWED_AT = CURRENT_TIMESTAMP()
    WHERE QUEUE_ID = :p_queue_id;

    INSERT INTO PIPELINE_OPS.SCHEMA_DRIFT_LOG 
    (PIPELINE_NAME, TARGET_TABLE, DETECTED_COLUMN, DRIFT_ACTION, EXECUTION_STATUS, MESSAGE)
    VALUES (:v_target_table, :v_target_table, :v_column_name, 'REJECT_UI', 'QUARANTINED_TO_DLQ', 'Rejected via UI by ' || :p_user || '; sent to DLQ.');

    RETURN 'Rejected: Drift column ' || v_column_name || ' rejected. Offending records quarantined to DLQ.';
END;
$$;

-- PROCEDURE 5: INGEST DIM_ACCOUNTS (Customer Dimension Ingestion with Dynamic Schema Evolution)
CREATE OR REPLACE PROCEDURE PIPELINE_OPS.SP_INGEST_DIM_ACCOUNTS()
RETURNS INT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_rows INT DEFAULT 0;
    v_insert_cols VARCHAR(2000);
    v_select_cols VARCHAR(2000);
    v_update_cols VARCHAR(2000);
    v_select_proj VARCHAR(2000);
    v_merge_sql VARCHAR(4000);
BEGIN
    USE WAREHOUSE WH_PROD;

    -- Query all current columns in SILVER.DIM_ACCOUNTS (including any approved drift columns like INDUSTRY, REGION)
    CREATE OR REPLACE TEMPORARY TABLE PIPELINE_OPS.TEMP_DIM_ACC_COLS AS
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'SILVER' 
      AND TABLE_NAME = 'DIM_ACCOUNTS'
      AND COLUMN_NAME NOT IN ('ETL_LOADED_AT');

    SELECT 
        LISTAGG(COLUMN_NAME, ', '),
        LISTAGG('src.' || COLUMN_NAME, ', '),
        LISTAGG('tgt.' || COLUMN_NAME || ' = src.' || COLUMN_NAME, ', ')
    INTO 
        :v_insert_cols,
        :v_select_cols,
        :v_update_cols
    FROM PIPELINE_OPS.TEMP_DIM_ACC_COLS;

    SELECT LISTAGG(
        CASE 
            WHEN COLUMN_NAME = 'ACCOUNT_ID' THEN 'b.RAW_RECORD:account_id::VARCHAR AS ACCOUNT_ID'
            WHEN COLUMN_NAME = 'ACCOUNT_NAME' THEN 'b.RAW_RECORD:name::VARCHAR AS ACCOUNT_NAME'
            WHEN COLUMN_NAME = 'TIER' THEN 'b.RAW_RECORD:tier::VARCHAR AS TIER'
            WHEN COLUMN_NAME = 'IS_DELETED' THEN 'COALESCE(b.RAW_RECORD:is_deleted::BOOLEAN, FALSE) AS IS_DELETED'
            WHEN COLUMN_NAME = 'UPDATED_AT' THEN 'TRY_TO_TIMESTAMP_NTZ(b.RAW_RECORD:updated_at::VARCHAR) AS UPDATED_AT'
            WHEN COLUMN_NAME LIKE '%_AT' OR COLUMN_NAME LIKE '%_TIME' 
                THEN 'TRY_TO_TIMESTAMP_NTZ(b.RAW_RECORD:' || LOWER(COLUMN_NAME) || '::VARCHAR) AS ' || COLUMN_NAME
            ELSE 'b.RAW_RECORD:' || LOWER(COLUMN_NAME) || '::VARCHAR AS ' || COLUMN_NAME
        END, ', ')
    INTO :v_select_proj
    FROM PIPELINE_OPS.TEMP_DIM_ACC_COLS;

    IF (v_insert_cols IS NOT NULL AND LENGTH(v_insert_cols) > 0) THEN
        v_merge_sql := 'MERGE INTO SILVER.DIM_ACCOUNTS tgt ' ||
                       'USING (' ||
                       '    SELECT ' || v_select_proj || ' ' ||
                       '    FROM BRONZE.OPENFLOW_RAW_STREAM b ' ||
                       '    WHERE b.PAYLOAD_TYPE = \'ACCOUNT\' AND b.RAW_RECORD:account_id IS NOT NULL ' ||
                       '      AND NOT EXISTS (' ||
                       '          SELECT 1 FROM PIPELINE_OPS.SCHEMA_DRIFT_APPROVAL_QUEUE q ' ||
                       '          WHERE q.PAYLOAD_TYPE = \'ACCOUNT\' ' ||
                       '            AND q.STATUS = \'REJECTED\' ' ||
                       '            AND b.RAW_RECORD[LOWER(q.DETECTED_COLUMN)] IS NOT NULL' ||
                       '      ) ' ||
                       '    QUALIFY ROW_NUMBER() OVER (PARTITION BY b.RAW_RECORD:account_id::VARCHAR ORDER BY b.RAW_RECORD:updated_at::TIMESTAMP_NTZ DESC NULLS LAST) = 1 ' ||
                       ') src ON tgt.ACCOUNT_ID = src.ACCOUNT_ID ' ||
                       'WHEN MATCHED AND (tgt.UPDATED_AT IS NULL OR src.UPDATED_AT >= tgt.UPDATED_AT) THEN UPDATE SET ' || v_update_cols || ' ' ||
                       'WHEN NOT MATCHED THEN INSERT (' || v_insert_cols || ') VALUES (' || v_select_cols || ');';

        EXECUTE IMMEDIATE :v_merge_sql;
        v_rows := SQLROWCOUNT;
    END IF;

    RETURN v_rows;
END;
$$;

-- PROCEDURE 6: INGEST DIM_PRODUCTS (Product Dimension Ingestion)
CREATE OR REPLACE PROCEDURE PIPELINE_OPS.SP_INGEST_DIM_PRODUCTS()
RETURNS INT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_rows INT DEFAULT 0;
BEGIN
    USE WAREHOUSE WH_PROD;

    MERGE INTO SILVER.DIM_PRODUCTS tgt
    USING (
        SELECT 
            RAW_RECORD:product_id::VARCHAR AS PRODUCT_ID,
            RAW_RECORD:name::VARCHAR AS PRODUCT_NAME,
            RAW_RECORD:category::VARCHAR AS CATEGORY,
            RAW_RECORD:price::NUMBER(10,2) AS UNIT_PRICE,
            RAW_RECORD:updated_at::TIMESTAMP_NTZ AS UPDATED_AT
        FROM BRONZE.OPENFLOW_RAW_STREAM
        WHERE PAYLOAD_TYPE = 'PRODUCT' AND RAW_RECORD:product_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY RAW_RECORD:product_id::VARCHAR ORDER BY RAW_RECORD:updated_at::TIMESTAMP_NTZ DESC NULLS LAST) = 1
    ) src ON tgt.PRODUCT_ID = src.PRODUCT_ID
    WHEN MATCHED AND (tgt.UPDATED_AT IS NULL OR src.UPDATED_AT >= tgt.UPDATED_AT) THEN
        UPDATE SET tgt.PRODUCT_NAME = src.PRODUCT_NAME, tgt.CATEGORY = src.CATEGORY, tgt.UNIT_PRICE = src.UNIT_PRICE, tgt.UPDATED_AT = src.UPDATED_AT
    WHEN NOT MATCHED THEN
        INSERT (PRODUCT_ID, PRODUCT_NAME, CATEGORY, UNIT_PRICE, UPDATED_AT)
        VALUES (src.PRODUCT_ID, src.PRODUCT_NAME, src.CATEGORY, src.UNIT_PRICE, src.UPDATED_AT);

    v_rows := SQLROWCOUNT;
    RETURN v_rows;
END;
$$;

-- PROCEDURE 7: INGEST FACT_ORDERS (Order Headers Ingestion with Dynamic Schema Evolution)
CREATE OR REPLACE PROCEDURE PIPELINE_OPS.SP_INGEST_FACT_ORDERS()
RETURNS INT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_rows INT DEFAULT 0;
    v_insert_cols VARCHAR(2000);
    v_select_cols VARCHAR(2000);
    v_update_cols VARCHAR(2000);
    v_select_proj VARCHAR(2000);
    v_merge_sql VARCHAR(4000);
BEGIN
    USE WAREHOUSE WH_PROD;

    CREATE OR REPLACE TEMPORARY TABLE PIPELINE_OPS.TEMP_FACT_ORD_COLS AS
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'SILVER' 
      AND TABLE_NAME = 'FACT_ORDERS'
      AND COLUMN_NAME NOT IN ('ETL_LOADED_AT');

    SELECT 
        LISTAGG(COLUMN_NAME, ', '),
        LISTAGG('src.' || COLUMN_NAME, ', '),
        LISTAGG('tgt.' || COLUMN_NAME || ' = src.' || COLUMN_NAME, ', ')
    INTO 
        :v_insert_cols,
        :v_select_cols,
        :v_update_cols
    FROM PIPELINE_OPS.TEMP_FACT_ORD_COLS;

    SELECT LISTAGG(
        CASE 
            WHEN COLUMN_NAME = 'ORDER_ID' THEN 'b.RAW_RECORD:order_id::INT AS ORDER_ID'
            WHEN COLUMN_NAME = 'CUSTOMER_ID' THEN 'b.RAW_RECORD:customer_id::VARCHAR AS CUSTOMER_ID'
            WHEN COLUMN_NAME = 'ORDER_STATUS' THEN 'b.RAW_RECORD:status::VARCHAR AS ORDER_STATUS'
            WHEN COLUMN_NAME = 'ORDER_TIMESTAMP' THEN 'TRY_TO_TIMESTAMP_NTZ(b.RAW_RECORD:updated_at::VARCHAR) AS ORDER_TIMESTAMP'
            WHEN COLUMN_NAME LIKE '%_AT' OR COLUMN_NAME LIKE '%_TIME' 
                THEN 'TRY_TO_TIMESTAMP_NTZ(b.RAW_RECORD:' || LOWER(COLUMN_NAME) || '::VARCHAR) AS ' || COLUMN_NAME
            ELSE 'b.RAW_RECORD:' || LOWER(COLUMN_NAME) || '::VARCHAR AS ' || COLUMN_NAME
        END, ', ')
    INTO :v_select_proj
    FROM PIPELINE_OPS.TEMP_FACT_ORD_COLS;

    IF (v_insert_cols IS NOT NULL AND LENGTH(v_insert_cols) > 0) THEN
        v_merge_sql := 'MERGE INTO SILVER.FACT_ORDERS tgt ' ||
                       'USING (' ||
                       '    SELECT ' || v_select_proj || ' ' ||
                       '    FROM BRONZE.OPENFLOW_RAW_STREAM b ' ||
                       '    WHERE b.PAYLOAD_TYPE = \'ORDER\' AND b.RAW_RECORD:order_id::INT > 0 ' ||
                       '      AND NOT EXISTS (' ||
                       '          SELECT 1 FROM PIPELINE_OPS.SCHEMA_DRIFT_APPROVAL_QUEUE q ' ||
                       '          WHERE q.PAYLOAD_TYPE = \'ORDER\' ' ||
                       '            AND q.STATUS = \'REJECTED\' ' ||
                       '            AND b.RAW_RECORD[LOWER(q.DETECTED_COLUMN)] IS NOT NULL' ||
                       '      ) ' ||
                       '    QUALIFY ROW_NUMBER() OVER (PARTITION BY b.RAW_RECORD:order_id::INT ORDER BY b.RAW_RECORD:updated_at::TIMESTAMP_NTZ DESC NULLS LAST) = 1 ' ||
                       ') src ON tgt.ORDER_ID = src.ORDER_ID ' ||
                       'WHEN MATCHED THEN UPDATE SET ' || v_update_cols || ' ' ||
                       'WHEN NOT MATCHED THEN INSERT (' || v_insert_cols || ') VALUES (' || v_select_cols || ');';

        EXECUTE IMMEDIATE :v_merge_sql;
        v_rows := SQLROWCOUNT;
    END IF;

    RETURN v_rows;
END;
$$;

-- PROCEDURE 8: INGEST FACT_SALES (Controlled Array Flattening Ingestion)
CREATE OR REPLACE PROCEDURE PIPELINE_OPS.SP_INGEST_FACT_SALES(
    p_flatten_policy VARCHAR DEFAULT 'LATEST_ONLY'
)
RETURNS INT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_rows INT DEFAULT 0;
    v_policy VARCHAR(50);
BEGIN
    USE WAREHOUSE WH_PROD;
    v_policy := UPPER(COALESCE(:p_flatten_policy, 'LATEST_ONLY'));

    IF (v_policy = 'LATEST_ONLY') THEN
        MERGE INTO SILVER.FACT_SALES tgt
        USING (
            WITH combined_sales AS (
                -- 1. Direct Sales from SALE payloads
                SELECT 
                    RAW_RECORD:sale_id::INT AS SALE_ID,
                    RAW_RECORD:order_id::INT AS ORDER_ID,
                    RAW_RECORD:product_id::VARCHAR AS PRODUCT_ID,
                    RAW_RECORD:quantity::INT AS QUANTITY,
                    RAW_RECORD:unit_price::NUMBER(10,2) AS UNIT_PRICE,
                    RAW_RECORD:discount::NUMBER(10,2) AS DISCOUNT_AMOUNT,
                    RAW_RECORD:net_revenue::NUMBER(12,2) AS NET_REVENUE,
                    RAW_RECORD:payment_method::VARCHAR AS PAYMENT_METHOD,
                    RAW_RECORD:sale_time::TIMESTAMP_NTZ AS SALE_TIMESTAMP
                FROM BRONZE.OPENFLOW_RAW_STREAM
                WHERE PAYLOAD_TYPE = 'SALE' AND RAW_RECORD:sale_id::INT > 0
                
                UNION ALL
                
                -- 2. Flattened Line Items from ORDER payloads
                SELECT 
                    f.value:sale_id::INT AS SALE_ID,
                    b.RAW_RECORD:order_id::INT AS ORDER_ID,
                    f.value:product_id::VARCHAR AS PRODUCT_ID,
                    f.value:quantity::INT AS QUANTITY,
                    f.value:price::NUMBER(10,2) AS UNIT_PRICE,
                    f.value:discount::NUMBER(10,2) AS DISCOUNT_AMOUNT,
                    f.value:net_revenue::NUMBER(12,2) AS NET_REVENUE,
                    f.value:payment_method::VARCHAR AS PAYMENT_METHOD,
                    f.value:sale_time::TIMESTAMP_NTZ AS SALE_TIMESTAMP
                FROM BRONZE.OPENFLOW_RAW_STREAM b,
                LATERAL FLATTEN(input => b.RAW_RECORD:items) f
                WHERE b.PAYLOAD_TYPE = 'ORDER' 
                  AND IS_ARRAY(b.RAW_RECORD:items)
                  AND f.value:sale_id::INT > 0
            )
            SELECT 
                SALE_ID, ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, DISCOUNT_AMOUNT, NET_REVENUE, PAYMENT_METHOD, SALE_TIMESTAMP
            FROM combined_sales
            -- Deduplicate across the ENTIRE batch by SALE_ID (prevents duplicate rows in the MERGE source)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY SALE_TIMESTAMP DESC NULLS LAST) = 1
        ) src ON tgt.SALE_ID = src.SALE_ID
        WHEN MATCHED THEN
            UPDATE SET tgt.ORDER_ID = src.ORDER_ID, tgt.PRODUCT_ID = src.PRODUCT_ID, tgt.QUANTITY = src.QUANTITY, tgt.UNIT_PRICE = src.UNIT_PRICE, tgt.DISCOUNT_AMOUNT = src.DISCOUNT_AMOUNT, tgt.NET_REVENUE = src.NET_REVENUE, tgt.PAYMENT_METHOD = src.PAYMENT_METHOD, tgt.SALE_TIMESTAMP = src.SALE_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (SALE_ID, ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, DISCOUNT_AMOUNT, NET_REVENUE, PAYMENT_METHOD, SALE_TIMESTAMP)
            VALUES (src.SALE_ID, src.ORDER_ID, src.PRODUCT_ID, src.QUANTITY, src.UNIT_PRICE, src.DISCOUNT_AMOUNT, src.NET_REVENUE, src.PAYMENT_METHOD, src.SALE_TIMESTAMP);
            
        v_rows := SQLROWCOUNT;
    ELSE
        MERGE INTO SILVER.FACT_SALES tgt
        USING (
            WITH combined_sales AS (
                SELECT 
                    RAW_RECORD:sale_id::INT AS SALE_ID,
                    RAW_RECORD:order_id::INT AS ORDER_ID,
                    RAW_RECORD:product_id::VARCHAR AS PRODUCT_ID,
                    RAW_RECORD:quantity::INT AS QUANTITY,
                    RAW_RECORD:unit_price::NUMBER(10,2) AS UNIT_PRICE,
                    RAW_RECORD:discount::NUMBER(10,2) AS DISCOUNT_AMOUNT,
                    RAW_RECORD:net_revenue::NUMBER(12,2) AS NET_REVENUE,
                    RAW_RECORD:payment_method::VARCHAR AS PAYMENT_METHOD,
                    RAW_RECORD:sale_time::TIMESTAMP_NTZ AS SALE_TIMESTAMP
                FROM BRONZE.OPENFLOW_RAW_STREAM
                WHERE PAYLOAD_TYPE = 'SALE' AND RAW_RECORD:sale_id::INT > 0
                
                UNION ALL
                
                SELECT 
                    f.value:sale_id::INT AS SALE_ID,
                    b.RAW_RECORD:order_id::INT AS ORDER_ID,
                    f.value:product_id::VARCHAR AS PRODUCT_ID,
                    f.value:quantity::INT AS QUANTITY,
                    f.value:price::NUMBER(10,2) AS UNIT_PRICE,
                    f.value:discount::NUMBER(10,2) AS DISCOUNT_AMOUNT,
                    f.value:net_revenue::NUMBER(12,2) AS NET_REVENUE,
                    f.value:payment_method::VARCHAR AS PAYMENT_METHOD,
                    f.value:sale_time::TIMESTAMP_NTZ AS SALE_TIMESTAMP
                FROM BRONZE.OPENFLOW_RAW_STREAM b,
                LATERAL FLATTEN(input => b.RAW_RECORD:items) f
                WHERE b.PAYLOAD_TYPE = 'ORDER' 
                  AND IS_ARRAY(b.RAW_RECORD:items)
                  AND f.value:sale_id::INT > 0
            )
            SELECT 
                SALE_ID, ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, DISCOUNT_AMOUNT, NET_REVENUE, PAYMENT_METHOD, SALE_TIMESTAMP
            FROM combined_sales
            QUALIFY ROW_NUMBER() OVER (PARTITION BY SALE_ID ORDER BY SALE_TIMESTAMP DESC NULLS LAST) = 1
        ) src ON tgt.SALE_ID = src.SALE_ID
        WHEN MATCHED THEN
            UPDATE SET tgt.ORDER_ID = src.ORDER_ID, tgt.PRODUCT_ID = src.PRODUCT_ID, tgt.QUANTITY = src.QUANTITY, tgt.UNIT_PRICE = src.UNIT_PRICE, tgt.DISCOUNT_AMOUNT = src.DISCOUNT_AMOUNT, tgt.NET_REVENUE = src.NET_REVENUE, tgt.PAYMENT_METHOD = src.PAYMENT_METHOD, tgt.SALE_TIMESTAMP = src.SALE_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (SALE_ID, ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, DISCOUNT_AMOUNT, NET_REVENUE, PAYMENT_METHOD, SALE_TIMESTAMP)
            VALUES (src.SALE_ID, src.ORDER_ID, src.PRODUCT_ID, src.QUANTITY, src.UNIT_PRICE, src.DISCOUNT_AMOUNT, src.NET_REVENUE, src.PAYMENT_METHOD, src.SALE_TIMESTAMP);
            
        v_rows := SQLROWCOUNT;
    END IF;

    RETURN v_rows;
END;
$$;

-- PROCEDURE 9: INGEST DYNAMIC TABLE
CREATE OR REPLACE PROCEDURE PIPELINE_OPS.SP_INGEST_DYNAMIC_TABLE(
    p_payload_type VARCHAR
)
RETURNS INT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_rows INT DEFAULT 0;
    v_target_table VARCHAR(100);
    v_business_key VARCHAR(100);
    v_insert_cols VARCHAR(2000);
    v_select_cols VARCHAR(2000);
    v_update_cols VARCHAR(2000);
    v_select_proj VARCHAR(2000);
    v_merge_sql VARCHAR(4000);
BEGIN
    USE WAREHOUSE WH_PROD;

    SELECT TARGET_TABLE, BUSINESS_KEY
    INTO :v_target_table, :v_business_key
    FROM PIPELINE_OPS.SOURCE_INGESTION_CONTROL
    WHERE PAYLOAD_TYPE = UPPER(:p_payload_type) AND APPROVAL_STATUS = 'APPROVED';

    CREATE OR REPLACE TEMPORARY TABLE PIPELINE_OPS.TEMP_DYNAMIC_COLS AS
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'SILVER' 
      AND TABLE_NAME = UPPER(:v_target_table)
      AND COLUMN_NAME NOT IN ('ETL_LOADED_AT');

    SELECT 
        LISTAGG(COLUMN_NAME, ', '),
        LISTAGG('src.' || COLUMN_NAME, ', '),
        LISTAGG('tgt.' || COLUMN_NAME || ' = src.' || COLUMN_NAME, ', ')
    INTO 
        :v_insert_cols,
        :v_select_cols,
        :v_update_cols
    FROM PIPELINE_OPS.TEMP_DYNAMIC_COLS;

    SELECT LISTAGG(
        CASE 
            WHEN COLUMN_NAME LIKE '%_AT' OR COLUMN_NAME LIKE '%_TIME' 
                THEN 'TRY_TO_TIMESTAMP_NTZ(b.RAW_RECORD:' || LOWER(COLUMN_NAME) || '::VARCHAR) AS ' || COLUMN_NAME
            ELSE 'b.RAW_RECORD:' || LOWER(COLUMN_NAME) || '::VARCHAR AS ' || COLUMN_NAME
        END, ', ')
    INTO :v_select_proj
    FROM PIPELINE_OPS.TEMP_DYNAMIC_COLS;

    IF (v_insert_cols IS NOT NULL AND LENGTH(v_insert_cols) > 0) THEN
        v_merge_sql := 'MERGE INTO SILVER.' || UPPER(v_target_table) || ' tgt ' ||
                       'USING (' ||
                       '    SELECT ' || v_select_proj || ' ' ||
                       '    FROM BRONZE.OPENFLOW_RAW_STREAM b ' ||
                       '    WHERE b.PAYLOAD_TYPE = \'' || UPPER(:p_payload_type) || '\' ' ||
                       '    QUALIFY ROW_NUMBER() OVER (PARTITION BY b.RAW_RECORD:' || LOWER(:v_business_key) || '::VARCHAR ORDER BY b.INGESTED_AT DESC) = 1 ' ||
                       ') src ON tgt.' || :v_business_key || ' = src.' || :v_business_key || ' ' ||
                       'WHEN MATCHED THEN UPDATE SET ' || v_update_cols || ' ' ||
                       'WHEN NOT MATCHED THEN INSERT (' || v_insert_cols || ') VALUES (' || v_select_cols || ');';

        EXECUTE IMMEDIATE :v_merge_sql;
        v_rows := SQLROWCOUNT;
    END IF;

    RETURN v_rows;
END;
$$;

-- PROCEDURE 10: RUN ALL INGESTION PIPELINES (Master Orchestration Runner)
CREATE OR REPLACE PROCEDURE PIPELINE_OPS.SP_RUN_ALL_INGESTION_PIPELINES(
    p_flatten_policy VARCHAR DEFAULT 'LATEST_ONLY'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_total_ingested INT DEFAULT 0;
    v_acc_cnt INT DEFAULT 0;
    v_prod_cnt INT DEFAULT 0;
    v_ord_cnt INT DEFAULT 0;
    v_sales_cnt INT DEFAULT 0;
    v_dynamic_cnt INT DEFAULT 0;
    v_cur_dyn_rows INT DEFAULT 0;
    v_cur_payload VARCHAR(100);
    v_policy VARCHAR(50);

    c_dynamic CURSOR FOR
        SELECT PAYLOAD_TYPE
        FROM PIPELINE_OPS.SOURCE_INGESTION_CONTROL
        WHERE IS_ACTIVE = TRUE 
          AND APPROVAL_STATUS = 'APPROVED'
          AND PAYLOAD_TYPE NOT IN ('ACCOUNT', 'PRODUCT', 'ORDER', 'SALE');
BEGIN
    USE WAREHOUSE WH_PROD;
    v_policy := UPPER(COALESCE(:p_flatten_policy, 'LATEST_ONLY'));

    CALL PIPELINE_OPS.SP_INGEST_DIM_ACCOUNTS() INTO :v_acc_cnt;
    CALL PIPELINE_OPS.SP_INGEST_DIM_PRODUCTS() INTO :v_prod_cnt;
    CALL PIPELINE_OPS.SP_INGEST_FACT_ORDERS() INTO :v_ord_cnt;
    CALL PIPELINE_OPS.SP_INGEST_FACT_SALES(:v_policy) INTO :v_sales_cnt;

    FOR rec IN c_dynamic DO
        v_cur_payload := rec.PAYLOAD_TYPE;
        CALL PIPELINE_OPS.SP_INGEST_DYNAMIC_TABLE(:v_cur_payload) INTO :v_cur_dyn_rows;
        v_dynamic_cnt := v_dynamic_cnt + v_cur_dyn_rows;
    END FOR;

    v_total_ingested := v_acc_cnt + v_prod_cnt + v_ord_cnt + v_sales_cnt + v_dynamic_cnt;

    INSERT INTO PIPELINE_OPS.PIPELINE_METRICS 
    (PIPELINE_NAME, SOURCE, SUCCESS_RECORDS, FAILED_RECORDS, THROUGHPUT_REC_SEC, FLATTENING_MODE)
    SELECT 'OPENFLOW_GOVERNED_PIPELINE', 'OPENFLOW_STREAM', :v_total_ingested, 0, ROUND(:v_total_ingested / 1.5, 2), :v_policy;

    RETURN 'Ingestion Complete (Policy: ' || v_policy || '). Total rows processed into Silver: ' || v_total_ingested ||
           ' [Accounts: ' || v_acc_cnt || ', Products: ' || v_prod_cnt || ', Orders: ' || v_ord_cnt || 
           ', Sales: ' || v_sales_cnt || ', Dynamic Tables: ' || v_dynamic_cnt || ']';
END;
$$;

-- ============================================================================
-- 7. AUTOMATED CDC TASK (Runs Every 5 Minutes When Delta Exists)
-- ============================================================================
CREATE OR REPLACE TASK PIPELINE_OPS.TASK_RUN_OPENFLOW_PIPELINES
    WAREHOUSE = 'WH_PROD'
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STR_OPENFLOW_STREAM')
AS
    CALL PIPELINE_OPS.SP_RUN_ALL_INGESTION_PIPELINES('LATEST_ONLY');

-- Task remains initially suspended; resume when ready:
-- ALTER TASK PIPELINE_OPS.TASK_RUN_OPENFLOW_PIPELINES RESUME;

-- ============================================================================
-- 8. COMPREHENSIVE ROLE PRIVILEGES (RBAC)
-- ============================================================================
USE ROLE SECURITYADMIN;

-- 1. Engineer Privileges
GRANT USAGE, OPERATE ON WAREHOUSE WH_DEV TO ROLE ROLE_DATA_ENGINEER;
GRANT USAGE, OPERATE ON WAREHOUSE WH_PROD TO ROLE ROLE_DATA_ENGINEER;
GRANT USAGE, OPERATE ON WAREHOUSE WH_TEST TO ROLE ROLE_DATA_ENGINEER;
GRANT USAGE ON DATABASE RETAIL_ENTERPRISE_DB TO ROLE ROLE_DATA_ENGINEER;

GRANT USAGE ON ALL SCHEMAS IN DATABASE RETAIL_ENTERPRISE_DB TO ROLE ROLE_DATA_ENGINEER;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE RETAIL_ENTERPRISE_DB TO ROLE ROLE_DATA_ENGINEER;

GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT, CREATE STREAM, CREATE TASK, CREATE PROCEDURE ON SCHEMA PIPELINE_OPS TO ROLE ROLE_DATA_ENGINEER;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT, CREATE STREAM ON SCHEMA BRONZE TO ROLE ROLE_DATA_ENGINEER;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA SILVER TO ROLE ROLE_DATA_ENGINEER;

GRANT USAGE, READ, WRITE ON STAGE BRONZE.STG_OPENFLOW_INGRESS TO ROLE ROLE_DATA_ENGINEER;
GRANT USAGE ON FILE FORMAT BRONZE.FF_CSV_PIPE TO ROLE ROLE_DATA_ENGINEER;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA BRONZE TO ROLE ROLE_DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA SILVER TO ROLE ROLE_DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA PIPELINE_OPS TO ROLE ROLE_DATA_ENGINEER;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA BRONZE TO ROLE ROLE_DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA SILVER TO ROLE ROLE_DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA PIPELINE_OPS TO ROLE ROLE_DATA_ENGINEER;

GRANT SELECT ON ALL STREAMS IN SCHEMA BRONZE TO ROLE ROLE_DATA_ENGINEER;
GRANT SELECT ON ALL VIEWS IN SCHEMA SILVER TO ROLE ROLE_DATA_ENGINEER;
GRANT SELECT ON ALL VIEWS IN SCHEMA PIPELINE_OPS TO ROLE ROLE_DATA_ENGINEER;
GRANT OPERATE, MONITOR ON ALL TASKS IN SCHEMA PIPELINE_OPS TO ROLE ROLE_DATA_ENGINEER;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA PIPELINE_OPS TO ROLE ROLE_DATA_ENGINEER;

-- 2. Analyst Privileges (Read-Only)
GRANT USAGE ON WAREHOUSE WH_TEST TO ROLE ROLE_ANALYST;
GRANT USAGE ON DATABASE RETAIL_ENTERPRISE_DB TO ROLE ROLE_ANALYST;
GRANT USAGE ON SCHEMA SILVER TO ROLE ROLE_ANALYST;
GRANT USAGE ON SCHEMA BRONZE TO ROLE ROLE_ANALYST;
GRANT USAGE ON SCHEMA PIPELINE_OPS TO ROLE ROLE_ANALYST;

GRANT SELECT ON ALL TABLES IN SCHEMA BRONZE TO ROLE ROLE_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA SILVER TO ROLE ROLE_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA BRONZE TO ROLE ROLE_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA SILVER TO ROLE ROLE_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA SILVER TO ROLE ROLE_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA PIPELINE_OPS TO ROLE ROLE_ANALYST;
GRANT MONITOR ON ALL TASKS IN SCHEMA PIPELINE_OPS TO ROLE ROLE_ANALYST;
GRANT READ ON STAGE BRONZE.STG_OPENFLOW_INGRESS TO ROLE ROLE_ANALYST;

-- Done! The entire OpenFlow framework is now deployed cleanly and portably.


USE ROLE ROLE_DATA_ENGINEER;
USE WAREHOUSE WH_DEV;
USE DATABASE RETAIL_ENTERPRISE_DB;

-- 1. Truncate the 4 Pre-Coded Core Tables in SILVER
TRUNCATE TABLE IF EXISTS SILVER.DIM_ACCOUNTS;
TRUNCATE TABLE IF EXISTS SILVER.DIM_PRODUCTS;
TRUNCATE TABLE IF EXISTS SILVER.FACT_ORDERS;
TRUNCATE TABLE IF EXISTS SILVER.FACT_SALES;

-- 2. (Optional) Truncate any dynamically onboarded tables if you created them
TRUNCATE TABLE IF EXISTS SILVER.SUPPLIER;
TRUNCATE TABLE IF EXISTS SILVER.WAREHOUSE;
TRUNCATE TABLE IF EXISTS SILVER.SHIPMENT;

-- 3. (Optional) Clear the Bronze Raw Stream & DLQ for a completely clean test run
TRUNCATE TABLE IF EXISTS BRONZE.OPENFLOW_RAW_STREAM;
TRUNCATE TABLE IF EXISTS PIPELINE_OPS.DEAD_LETTER_QUEUE;
TRUNCATE TABLE IF EXISTS PIPELINE_OPS.PIPELINE_METRICS;

-- Verify all tables are empty
SELECT 'DIM_ACCOUNTS' AS TBL, COUNT(*) AS ROW_CNT FROM SILVER.DIM_ACCOUNTS
UNION ALL
SELECT 'DIM_PRODUCTS', COUNT(*) FROM SILVER.DIM_PRODUCTS
UNION ALL
SELECT 'FACT_ORDERS', COUNT(*) FROM SILVER.FACT_ORDERS
UNION ALL
SELECT 'FACT_SALES', COUNT(*) FROM SILVER.FACT_SALES;

