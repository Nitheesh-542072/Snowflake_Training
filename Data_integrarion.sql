
-- [A] Security & Access Control: Create Custom Roles & Hierarchy
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS ROLE_DATA_ENGINEER
    COMMENT = 'Data Engineering role for managing ingestion, pipelines, and ETL';

CREATE ROLE IF NOT EXISTS ROLE_ANALYST
    COMMENT = 'Business Analyst role for querying Silver dimension and fact tables';

-- Role Inheritance: Integrate with Snowflake standard administrative hierarchy
GRANT ROLE ROLE_DATA_ENGINEER TO ROLE SYSADMIN;
GRANT ROLE ROLE_ANALYST TO ROLE SYSADMIN;

-- Grant custom roles to current user executing this script so role switches succeed
SET current_user_name = CURRENT_USER();
GRANT ROLE ROLE_DATA_ENGINEER TO USER IDENTIFIER($current_user_name);
GRANT ROLE ROLE_ANALYST TO USER IDENTIFIER($current_user_name);

-- [B] Resource Management: Governance & Cost Control
USE ROLE ACCOUNTADMIN;

-- Create Resource Monitor to protect against runaway credit consumption
CREATE OR REPLACE RESOURCE MONITOR RM_PROD
    WITH CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS 
        ON 80 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

-- Grant Monitor privileges to SYSADMIN (to assign to WH) and Data Engineers (to monitor)
GRANT MODIFY, MONITOR ON RESOURCE MONITOR RM_PROD TO ROLE SYSADMIN;
GRANT MONITOR ON RESOURCE MONITOR RM_PROD TO ROLE ROLE_DATA_ENGINEER;

-- Grant task execution privilege to Data Engineer role
GRANT EXECUTE TASK ON ACCOUNT TO ROLE ROLE_DATA_ENGINEER;

-- [C] Virtual Compute Warehouses: Multi-Tier Dedicated Workloads
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
    COMMENT = 'Production warehouse assigned to RM_PROD for CDC automated tasks, dynamic tables, and SP';

CREATE WAREHOUSE IF NOT EXISTS WH_TEST
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Testing and analyst query warehouse';

-- Set default active warehouse for subsequent object creation
USE WAREHOUSE WH_DEV;

-- ============================================================================
-- 1. DATABASE & ARCHITECTURE SCHEMAS
-- ============================================================================
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS RETAIL_ENTERPRISE_DB;
USE DATABASE RETAIL_ENTERPRISE_DB;

CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS PIPELINE_OPS;
DROP SCHEMA IF EXISTS GOLD CASCADE; -- Ensures previous Gold layer is dropped; architecture operates through Silver

-- ============================================================================
-- 2. NOTIFICATION INTEGRATION
-- ============================================================================
CREATE OR REPLACE NOTIFICATION INTEGRATION EMAIL_ALERT_INTEGRATION
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('nitheesh.padarthi@mastechdigital.com');

GRANT USAGE ON INTEGRATION EMAIL_ALERT_INTEGRATION TO ROLE ROLE_DATA_ENGINEER;
GRANT MONITOR ON RESOURCE MONITOR RM_PROD TO ROLE ROLE_DATA_ENGINEER;

-- ============================================================================
-- 3. PIPELINE OPERATIONS SCHEMA (Metadata & Control Plane)
-- ============================================================================
USE SCHEMA PIPELINE_OPS;

CREATE OR REPLACE TABLE WATERMARKS (
    PIPELINE_NAME VARCHAR(50) PRIMARY KEY,
    LAST_WATERMARK TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE DEAD_LETTER_QUEUE (
    ERROR_ID INT AUTOINCREMENT PRIMARY KEY,
    PIPELINE_NAME VARCHAR(50),
    RAW_PAYLOAD VARIANT,
    ERROR_MSG VARCHAR(200),
    RETRY_COUNT INT DEFAULT 0,
    STATUS VARCHAR(20) DEFAULT 'QUEUED'
);

-- ============================================================================
-- METADATA CONTROL PLANE: SOURCE INGESTION CONTROL (Zero-DDL Dynamic Onboarding)
-- ============================================================================
CREATE OR REPLACE TABLE SOURCE_INGESTION_CONTROL (
    SOURCE_ID INT AUTOINCREMENT PRIMARY KEY,
    SOURCE_NAME VARCHAR(100),
    PAYLOAD_TYPE VARCHAR(50) UNIQUE,
    TARGET_SCHEMA VARCHAR(50) DEFAULT 'SILVER',
    TARGET_TABLE VARCHAR(50),
    BUSINESS_KEY VARCHAR(50),
    TIE_BREAKER_COL VARCHAR(50) DEFAULT 'UPDATED_AT',
    SCHEMA_DRIFT_ACTION VARCHAR(20) DEFAULT 'ADD', -- 'ADD' or 'ALERT'
    AUTO_CREATE_TABLE BOOLEAN DEFAULT FALSE,       -- TRUE triggers runtime table creation
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Seed Active Core Pipelines (Known Sources):
-- ACCOUNT, PRODUCT, ORDER, SALE
-- Any other incoming table (e.g. SUPPLIER, VENDOR, WAREHOUSE) will be dynamically auto-discovered and created at runtime!
MERGE INTO SOURCE_INGESTION_CONTROL tgt
USING (
    SELECT 'SALESFORCE_CRM' AS S_NAME, 'ACCOUNT'  AS P_TYPE, 'SILVER' AS T_SCH, 'DIM_ACCOUNTS'  AS T_TBL, 'ACCOUNT_ID'  AS B_KEY, 'UPDATED_AT' AS TB_COL, 'ADD'   AS D_ACT, FALSE AS A_CRT UNION ALL
    SELECT 'ERP_CATALOG'    AS S_NAME, 'PRODUCT'  AS P_TYPE, 'SILVER' AS T_SCH, 'DIM_PRODUCTS'  AS T_TBL, 'PRODUCT_ID'  AS B_KEY, 'UPDATED_AT' AS TB_COL, 'ADD'   AS D_ACT, FALSE AS A_CRT UNION ALL
    SELECT 'ORDER_SERVICE'  AS S_NAME, 'ORDER'    AS P_TYPE, 'SILVER' AS T_SCH, 'FACT_ORDERS'   AS T_TBL, 'ORDER_ID'    AS B_KEY, 'UPDATED_AT' AS TB_COL, 'ALERT' AS D_ACT, FALSE AS A_CRT UNION ALL
    SELECT 'POS_CHECKOUT'   AS S_NAME, 'SALE'     AS P_TYPE, 'SILVER' AS T_SCH, 'FACT_SALES'    AS T_TBL, 'SALE_ID'     AS B_KEY, 'SALE_TIME'  AS TB_COL, 'ALERT' AS D_ACT, FALSE AS A_CRT
) src ON tgt.PAYLOAD_TYPE = src.P_TYPE
WHEN MATCHED THEN 
    UPDATE SET tgt.SOURCE_NAME = src.S_NAME, tgt.TARGET_SCHEMA = src.T_SCH, tgt.TARGET_TABLE = src.T_TBL, tgt.BUSINESS_KEY = src.B_KEY, tgt.TIE_BREAKER_COL = src.TB_COL, tgt.SCHEMA_DRIFT_ACTION = src.D_ACT, tgt.AUTO_CREATE_TABLE = src.A_CRT
WHEN NOT MATCHED THEN 
    INSERT (SOURCE_NAME, PAYLOAD_TYPE, TARGET_SCHEMA, TARGET_TABLE, BUSINESS_KEY, TIE_BREAKER_COL, SCHEMA_DRIFT_ACTION, AUTO_CREATE_TABLE)
    VALUES (src.S_NAME, src.P_TYPE, src.T_SCH, src.T_TBL, src.B_KEY, src.TB_COL, src.D_ACT, src.A_CRT);


select * from pipeline_ops.schema_evolution_control;

-- Schema Drift Governance Control Table: Configurable Policy per Entity ('ADD' or 'ALERT')
CREATE OR REPLACE TABLE SCHEMA_EVOLUTION_CONTROL (
    PIPELINE_NAME VARCHAR(50) PRIMARY KEY,
    TARGET_SCHEMA VARCHAR(50) DEFAULT 'SILVER',
    TARGET_TABLE VARCHAR(50),
    SCHEMA_DRIFT_ACTION VARCHAR(20) DEFAULT 'ADD', -- 'ADD' (auto-alter table) or 'ALERT' (quarantine to DLQ)
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

MERGE INTO SCHEMA_EVOLUTION_CONTROL tgt
USING (
    SELECT 'ACCOUNT' AS P, 'SILVER' AS S, 'DIM_ACCOUNTS' AS T, 'ADD'   AS A UNION ALL
    SELECT 'PRODUCT' AS P, 'SILVER' AS S, 'DIM_PRODUCTS' AS T, 'ADD'   AS A UNION ALL
    SELECT 'ORDER'   AS P, 'SILVER' AS S, 'FACT_ORDERS'  AS T, 'ALERT' AS A UNION ALL
    SELECT 'SALE'    AS P, 'SILVER' AS S, 'FACT_SALES'   AS T, 'ALERT' AS A
) src ON tgt.PIPELINE_NAME = src.P
WHEN MATCHED THEN UPDATE SET tgt.TARGET_SCHEMA = src.S, tgt.TARGET_TABLE = src.T, tgt.SCHEMA_DRIFT_ACTION = src.A
WHEN NOT MATCHED THEN INSERT (PIPELINE_NAME, TARGET_SCHEMA, TARGET_TABLE, SCHEMA_DRIFT_ACTION) VALUES (src.P, src.S, src.T, src.A);

-- Schema Drift Audit Log: Tracks all runtime drift events and actions taken
CREATE OR REPLACE TABLE SCHEMA_DRIFT_LOG (
    LOG_ID INT AUTOINCREMENT PRIMARY KEY,
    PIPELINE_NAME VARCHAR(50),
    TARGET_TABLE VARCHAR(50),
    DETECTED_COLUMN VARCHAR(100),
    DRIFT_ACTION VARCHAR(20),       -- 'ADD' or 'ALERT'
    EXECUTION_STATUS VARCHAR(30),   -- 'ALTER_SUCCESS' or 'QUARANTINED_TO_DLQ'
    MESSAGE VARCHAR(500),
    LOGGED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Observability Table tracking execution health, throughput, schema drift, and flattening mode
CREATE OR REPLACE TABLE PIPELINE_METRICS (
    PIPELINE_NAME VARCHAR(50),
    SOURCE VARCHAR(50),
    LAST_RUN TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SUCCESS_RECORDS INT DEFAULT 0,
    FAILED_RECORDS INT DEFAULT 0,
    THROUGHPUT_REC_SEC NUMBER(10,2) DEFAULT 0,
    SCHEMA_DRIFT_COUNT INT DEFAULT 0,
    FLATTENING_MODE VARCHAR(50) DEFAULT 'DIRECT'
);

-- Real-Time Operational Monitoring Dashboard View with Visual Health Badges
CREATE OR REPLACE VIEW V_MONITORING_DASHBOARD AS
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
    SCHEMA_DRIFT_COUNT AS "Drift Events",
    FLATTENING_MODE    AS "Flatten Mode",
    CASE 
        WHEN FAILED_RECORDS = 0 THEN '🟢 HEALTHY'
        WHEN (SUCCESS_RECORDS / NULLIF(SUCCESS_RECORDS + FAILED_RECORDS, 0)) >= 0.80 THEN '🟡 WARNING'
        ELSE '🔴 CRITICAL'
    END                AS "Health Status"
FROM PIPELINE_OPS.PIPELINE_METRICS;

-- ============================================================================
-- 4. BRONZE LAYER (Transient Raw Landing with Internal Stage & CDC Stream)
-- ============================================================================
USE SCHEMA BRONZE;

-- [A] File Format for Pipe-Delimited CSV Ingress
CREATE OR REPLACE FILE FORMAT BRONZE.FF_CSV_PIPE
    TYPE = 'CSV'
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE'
    NULL_IF = ('NULL', '')
    TRIM_SPACE = TRUE
    COMMENT = 'Pipe-delimited CSV format for raw JSON payload ingress';

-- [B] Named Internal Stage for File Uploads
CREATE OR REPLACE STAGE BRONZE.STG_OPENFLOW_INGRESS
    DIRECTORY = (ENABLE = TRUE)
    FILE_FORMAT = BRONZE.FF_CSV_PIPE
    COMMENT = 'Internal named stage for landing raw source files';

-- [C] Unified Raw Ingress Table for JSON Payloads (Auto-purges after 24 hrs to save storage)
CREATE OR REPLACE TRANSIENT TABLE BRONZE.OPENFLOW_RAW_STREAM (
    PAYLOAD_TYPE VARCHAR(50),    -- 'ACCOUNT', 'PRODUCT', 'ORDER', 'SALE'
    RAW_RECORD VARIANT,          -- Raw JSON Data
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) DATA_RETENTION_TIME_IN_DAYS = 1;

-- [D] Change Data Capture (CDC) Stream tracking unprocessed delta rows
CREATE OR REPLACE STREAM BRONZE.STR_OPENFLOW_STREAM 
ON TABLE BRONZE.OPENFLOW_RAW_STREAM;

-- ============================================================================
-- 5. SILVER LAYER: STAR SCHEMA (Dimensions & Facts with Foreign Keys)
-- ============================================================================
USE SCHEMA SILVER;

-- ----------------------------------------------------------------------------
-- [DIMENSION 1] Customer Accounts Dimension
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.DIM_ACCOUNTS (
    ACCOUNT_ID VARCHAR(50),
    ACCOUNT_NAME VARCHAR(150),
    TIER VARCHAR(20),                -- 'PREMIUM', 'STANDARD', 'ENTERPRISE'
    IS_DELETED BOOLEAN DEFAULT FALSE,
    UPDATED_AT TIMESTAMP_NTZ,
    ETL_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DIM_ACCOUNTS PRIMARY KEY (ACCOUNT_ID)
) 
COMMENT = 'DIMENSION TABLE: Customer Account Profiles and Membership Tiers'
DATA_RETENTION_TIME_IN_DAYS = 7;

-- ----------------------------------------------------------------------------
-- [DIMENSION 2] Products Catalog Dimension
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.DIM_PRODUCTS (
    PRODUCT_ID VARCHAR(50),
    PRODUCT_NAME VARCHAR(150),
    CATEGORY VARCHAR(50),
    UNIT_PRICE NUMBER(10,2),
    UPDATED_AT TIMESTAMP_NTZ,
    ETL_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DIM_PRODUCTS PRIMARY KEY (PRODUCT_ID)
) 
COMMENT = 'DIMENSION TABLE: Product Catalog Master and Unit Pricing'
DATA_RETENTION_TIME_IN_DAYS = 7;

-- ----------------------------------------------------------------------------
-- [FACT 1] Orders Header Fact (Relationship to DIM_ACCOUNTS)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.FACT_ORDERS (
    ORDER_ID INT,
    CUSTOMER_ID VARCHAR(50),
    ORDER_STATUS VARCHAR(30),        -- 'COMPLETED', 'PENDING', 'CANCELLED'
    ORDER_TIMESTAMP TIMESTAMP_NTZ,
    ETL_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_FACT_ORDERS PRIMARY KEY (ORDER_ID),
    CONSTRAINT FK_FACT_ORDERS_CUSTOMER FOREIGN KEY (CUSTOMER_ID) REFERENCES SILVER.DIM_ACCOUNTS(ACCOUNT_ID)
) 
COMMENT = 'FACT TABLE: Order Headers tracking transaction status and customer links'
DATA_RETENTION_TIME_IN_DAYS = 7;

-- ----------------------------------------------------------------------------
-- [FACT 2] Sales Line Items Fact (Relationship to FACT_ORDERS and DIM_PRODUCTS)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.FACT_SALES (
    SALE_ID INT,
    ORDER_ID INT,
    PRODUCT_ID VARCHAR(50),
    QUANTITY INT,
    UNIT_PRICE NUMBER(10,2),
    DISCOUNT_AMOUNT NUMBER(10,2),
    NET_REVENUE NUMBER(12,2),
    PAYMENT_METHOD VARCHAR(30),      -- 'CREDIT_CARD', 'NET_BANKING', 'UPI'
    SALE_TIMESTAMP TIMESTAMP_NTZ,
    ETL_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_FACT_SALES PRIMARY KEY (SALE_ID),
    CONSTRAINT FK_FACT_SALES_ORDER FOREIGN KEY (ORDER_ID) REFERENCES SILVER.FACT_ORDERS(ORDER_ID),
    CONSTRAINT FK_FACT_SALES_PRODUCT FOREIGN KEY (PRODUCT_ID) REFERENCES SILVER.DIM_PRODUCTS(PRODUCT_ID)
) 
COMMENT = 'FACT TABLE: Sales Line Items Grain tracking item quantities, discounts, and revenue'
DATA_RETENTION_TIME_IN_DAYS = 7;

-- Backward-compatible views (supports queries referencing original table names)
CREATE OR REPLACE VIEW SILVER.ACCOUNTS AS SELECT * FROM SILVER.DIM_ACCOUNTS;
CREATE OR REPLACE VIEW SILVER.PRODUCTS AS SELECT * FROM SILVER.DIM_PRODUCTS;
CREATE OR REPLACE VIEW SILVER.ORDERS   AS SELECT * FROM SILVER.FACT_ORDERS;
CREATE OR REPLACE VIEW SILVER.SALES    AS SELECT * FROM SILVER.FACT_SALES;

-- Data Masking for Customer Privacy on Dimension
CREATE OR REPLACE MASKING POLICY SILVER.MASK_ACCOUNT_NAME AS (val VARCHAR) RETURNS VARCHAR ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ROLE_DATA_ENGINEER') THEN val
        ELSE LEFT(val, 2) || '**** (Masked)'
    END;

ALTER TABLE SILVER.DIM_ACCOUNTS MODIFY COLUMN ACCOUNT_NAME SET MASKING POLICY SILVER.MASK_ACCOUNT_NAME;

-- ============================================================================
-- 6. ORCHESTRATION STORED PROCEDURE (Triggered Automatically on CDC Delta)
-- ============================================================================
CREATE OR REPLACE PROCEDURE PIPELINE_OPS.SP_RUN_OPENFLOW_PIPELINES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_watermark TIMESTAMP_NTZ;
    v_max_ts TIMESTAMP_NTZ;
    
    v_batch_total INT DEFAULT 0;
    v_acc_cnt INT DEFAULT 0;
    v_prod_cnt INT DEFAULT 0;
    v_ord_success INT DEFAULT 0;
    v_ord_fail INT DEFAULT 0;
    v_sales_cnt INT DEFAULT 0;
    
    -- Metadata-Driven Control Plane variables (Zero-DDL Onboarding)
    v_onboard_batch_cnt INT DEFAULT 0;
    v_table_exists INT DEFAULT 0;
    v_tables_created_cnt INT DEFAULT 0;
    v_onboard_records_loaded INT DEFAULT 0;
    v_col_defs VARCHAR(4000);
    v_create_tbl_sql VARCHAR(4000);
    v_insert_cols VARCHAR(2000);
    v_select_cols VARCHAR(2000);
    v_update_cols VARCHAR(2000);
    v_select_proj VARCHAR(2000);
    v_merge_sql VARCHAR(4000);

    -- Dynamic cursor iteration scalar binding variables (avoids invalid identifier in SQL)
    v_cur_src_name VARCHAR(100);
    v_cur_payload_type VARCHAR(100);
    v_cur_target_schema VARCHAR(100);
    v_cur_target_table VARCHAR(100);
    v_cur_business_key VARCHAR(100);
    v_cur_tie_breaker_col VARCHAR(100);
    v_cur_schema_drift_action VARCHAR(50);

    v_drift_payload_type VARCHAR(100);
    v_drift_target_schema VARCHAR(100);
    v_drift_target_table VARCHAR(100);
    v_drift_action VARCHAR(50);
    v_drift_incoming_col VARCHAR(100);
    v_drift_inferred_type VARCHAR(100);

    v_upd_payload_type VARCHAR(100);
    v_upd_target_schema VARCHAR(100);
    v_upd_target_table VARCHAR(100);
    v_upd_incoming_col VARCHAR(100);
    v_upd_business_key VARCHAR(100);

    -- Schema Drift Governance variables
    v_drift_detected_cnt INT DEFAULT 0;
    v_drift_added_cnt INT DEFAULT 0;
    v_drift_alert_cnt INT DEFAULT 0;
    v_alter_sql VARCHAR(1000);
    v_update_sql VARCHAR(1000);

    -- Semi-Structured Nested JSON & Conditional Flattening variables
    v_nested_orders_count INT DEFAULT 0;
    v_flatten_mode VARCHAR(50) DEFAULT 'DIRECT_NO_FLATTEN';

    -- Cursor declarations for FOR loops (dot notation requires named cursors)
    c_onboard CURSOR FOR
        SELECT SOURCE_NAME, PAYLOAD_TYPE, TARGET_SCHEMA, TARGET_TABLE, BUSINESS_KEY, TIE_BREAKER_COL, SCHEMA_DRIFT_ACTION
        FROM PIPELINE_OPS.SOURCE_INGESTION_CONTROL
        WHERE IS_ACTIVE = TRUE AND AUTO_CREATE_TABLE = TRUE;
    c_drift CURSOR FOR
        SELECT PAYLOAD_TYPE, TARGET_SCHEMA, TARGET_TABLE, SCHEMA_DRIFT_ACTION, INCOMING_COL, INFERRED_DATA_TYPE
        FROM PIPELINE_OPS.TEMP_INCOMING_DRIFT_CANDIDATES;
    c_update CURSOR FOR
        SELECT d.PAYLOAD_TYPE, d.TARGET_SCHEMA, d.TARGET_TABLE, d.INCOMING_COL, c.BUSINESS_KEY
        FROM PIPELINE_OPS.TEMP_INCOMING_DRIFT_CANDIDATES d
        JOIN PIPELINE_OPS.SOURCE_INGESTION_CONTROL c ON d.PAYLOAD_TYPE = c.PAYLOAD_TYPE
        WHERE d.SCHEMA_DRIFT_ACTION = 'ADD';

    v_wh_name VARCHAR DEFAULT 'WH_PROD';
    v_credit_quota FLOAT DEFAULT 50.0;
    v_credits_used FLOAT DEFAULT 0.0;
    v_credits_left FLOAT DEFAULT 50.0;
    v_usage_pct FLOAT DEFAULT 0.0;
    v_email_body VARCHAR(4000);
    v_email_status VARCHAR DEFAULT 'SUCCESS';
BEGIN
    USE WAREHOUSE WH_PROD;

    -- [0] RATE-LIMITED DELTA EXTRACTION: Snapshot up to 100 newly copied rows from Stream
    -- This single statement consumes the stream delta and resets SYSTEM$STREAM_HAS_DATA
    CREATE OR REPLACE TEMPORARY TABLE PIPELINE_OPS.TEMP_INGRESS_BATCH AS
    SELECT 
        PAYLOAD_TYPE, 
        RAW_RECORD, 
        INGESTED_AT
    FROM BRONZE.STR_OPENFLOW_STREAM
    WHERE METADATA$ACTION = 'INSERT'
    LIMIT 100; -- Rate-limit volume to max 100 records per execution

    SELECT COUNT(*) INTO :v_batch_total FROM PIPELINE_OPS.TEMP_INGRESS_BATCH;

    -- ========================================================================
    -- [1] METADATA-DRIVEN CONTROL PLANE: ONBOARD NEW SOURCE TABLES DYNAMICALLY
    -- Scans SOURCE_INGESTION_CONTROL for entities with AUTO_CREATE_TABLE = TRUE.
    -- If target table does not exist, dynamically creates it at runtime with zero manual DDL!
    -- Then dynamically merges the incoming data using configured BUSINESS_KEY.
    -- ========================================================================
    -- [1A] AUTO-DISCOVERY: Automatically register any brand new unconfigured payload types
    CREATE OR REPLACE TEMPORARY TABLE PIPELINE_OPS.TEMP_JUST_CREATED_TABLES (TABLE_NAME VARCHAR(100));

    INSERT INTO PIPELINE_OPS.SOURCE_INGESTION_CONTROL 
    (SOURCE_NAME, PAYLOAD_TYPE, TARGET_SCHEMA, TARGET_TABLE, BUSINESS_KEY, TIE_BREAKER_COL, SCHEMA_DRIFT_ACTION, AUTO_CREATE_TABLE, IS_ACTIVE)
    WITH distinct_new_sources AS (
        SELECT 
            b.PAYLOAD_TYPE,
            b.RAW_RECORD
        FROM PIPELINE_OPS.TEMP_INGRESS_BATCH b
        WHERE NOT EXISTS (
            SELECT 1 FROM PIPELINE_OPS.SOURCE_INGESTION_CONTROL c 
            WHERE c.PAYLOAD_TYPE = b.PAYLOAD_TYPE
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY b.PAYLOAD_TYPE ORDER BY b.INGESTED_AT DESC) = 1
    ),
    flattened_keys AS (
        SELECT
            s.PAYLOAD_TYPE,
            UPPER(f.KEY::VARCHAR) AS KEY_NAME
        FROM distinct_new_sources s,
             LATERAL FLATTEN(input => s.RAW_RECORD) f
    ),
    derived_keys AS (
        SELECT
            PAYLOAD_TYPE,
            MIN(CASE WHEN KEY_NAME LIKE '%_ID' OR KEY_NAME = 'ID' THEN KEY_NAME END) AS ID_KEY,
            MIN(KEY_NAME) AS FIRST_KEY,
            MIN(CASE WHEN KEY_NAME LIKE '%_AT' OR KEY_NAME LIKE '%_TIME' THEN KEY_NAME END) AS TS_KEY
        FROM flattened_keys
        GROUP BY PAYLOAD_TYPE
    )
    SELECT 
        'AUTODISCOVERED_' || dk.PAYLOAD_TYPE AS SOURCE_NAME,
        dk.PAYLOAD_TYPE,
        'SILVER' AS TARGET_SCHEMA,
        UPPER(dk.PAYLOAD_TYPE) AS TARGET_TABLE,
        COALESCE(dk.ID_KEY, dk.FIRST_KEY) AS BUSINESS_KEY,
        COALESCE(dk.TS_KEY, 'UPDATED_AT') AS TIE_BREAKER_COL,
        'ADD' AS SCHEMA_DRIFT_ACTION,
        TRUE AS AUTO_CREATE_TABLE,
        TRUE AS IS_ACTIVE
    FROM derived_keys dk;

    -- [1B] DYNAMIC ONBOARDING & SCD-1 MERGE FOR ALL ACTIVE ONBOARDED SOURCES
    OPEN c_onboard;
    FOR r_onboard IN c_onboard DO
        v_cur_src_name := r_onboard.SOURCE_NAME;
        v_cur_payload_type := r_onboard.PAYLOAD_TYPE;
        v_cur_target_schema := r_onboard.TARGET_SCHEMA;
        v_cur_target_table := r_onboard.TARGET_TABLE;
        v_cur_business_key := r_onboard.BUSINESS_KEY;
        v_cur_tie_breaker_col := r_onboard.TIE_BREAKER_COL;
        v_cur_schema_drift_action := r_onboard.SCHEMA_DRIFT_ACTION;

        SELECT COUNT(*) INTO :v_onboard_batch_cnt
        FROM PIPELINE_OPS.TEMP_INGRESS_BATCH
        WHERE PAYLOAD_TYPE = :v_cur_payload_type;

        IF (v_onboard_batch_cnt > 0) THEN
            SELECT COUNT(*) INTO :v_table_exists
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = :v_cur_target_schema
              AND TABLE_NAME = :v_cur_target_table;

            IF (v_table_exists = 0) THEN
                CREATE OR REPLACE TEMPORARY TABLE PIPELINE_OPS.TEMP_NEW_TABLE_COLS AS
                SELECT 
                    UPPER(f.KEY::VARCHAR) AS COL_NAME,
                    CASE 
                        WHEN UPPER(f.KEY::VARCHAR) = UPPER(:v_cur_business_key) THEN 'VARCHAR(50) PRIMARY KEY'
                        WHEN UPPER(f.KEY::VARCHAR) = UPPER(:v_cur_tie_breaker_col) OR UPPER(f.KEY::VARCHAR) LIKE '%_AT' OR UPPER(f.KEY::VARCHAR) LIKE '%_TIME' THEN 'TIMESTAMP_NTZ'
                        WHEN TYPEOF(f.VALUE) = 'INTEGER' THEN 'INT'
                        WHEN TYPEOF(f.VALUE) = 'DECIMAL' THEN 'NUMBER(12,2)'
                        WHEN TYPEOF(f.VALUE) = 'BOOLEAN' THEN 'BOOLEAN'
                        WHEN TYPEOF(f.VALUE) = 'ARRAY' THEN 'ARRAY'
                        WHEN TYPEOF(f.VALUE) = 'OBJECT' THEN 'VARIANT'
                        ELSE 'VARCHAR(150)'
                    END AS COL_TYPE
                FROM PIPELINE_OPS.TEMP_INGRESS_BATCH b,
                LATERAL FLATTEN(input => b.RAW_RECORD) f
                WHERE b.PAYLOAD_TYPE = :v_cur_payload_type
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY UPPER(f.KEY::VARCHAR) 
                    ORDER BY CASE WHEN TYPEOF(f.VALUE) = 'NULL_VALUE' THEN 2 ELSE 1 END
                ) = 1;

                SELECT LISTAGG(COL_NAME || ' ' || COL_TYPE, ',\n        ')
                INTO :v_col_defs
                FROM PIPELINE_OPS.TEMP_NEW_TABLE_COLS;

                v_create_tbl_sql := 'CREATE TABLE IF NOT EXISTS ' || v_cur_target_schema || '.' || v_cur_target_table || ' (' || CHAR(10) ||
                                    '        ' || v_col_defs || ', ' || CHAR(10) ||
                                    '        ETL_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()' || CHAR(10) ||
                                    ') COMMENT = \'Metadata-driven auto-onboarded table via SOURCE_INGESTION_CONTROL\' DATA_RETENTION_TIME_IN_DAYS = 7;';
                
                EXECUTE IMMEDIATE v_create_tbl_sql;

                INSERT INTO PIPELINE_OPS.TEMP_JUST_CREATED_TABLES (TABLE_NAME) VALUES (:v_cur_target_table);

                INSERT INTO PIPELINE_OPS.SCHEMA_DRIFT_LOG (PIPELINE_NAME, TARGET_TABLE, DETECTED_COLUMN, DRIFT_ACTION, EXECUTION_STATUS, MESSAGE)
                VALUES (:v_cur_payload_type, :v_cur_target_table, 'TABLE_CREATED', 'AUTO_ONBOARD', 'SUCCESS', 
                        'Dynamically created table ' || :v_cur_target_table || ' from SOURCE_INGESTION_CONTROL with zero manual DDL');
                
                v_tables_created_cnt := v_tables_created_cnt + 1;
            END IF;

            -- Dynamic SCD Type 1 MERGE into the newly created onboarded table
            CREATE OR REPLACE TEMPORARY TABLE PIPELINE_OPS.TEMP_ONBOARD_MERGE_COLS AS
            SELECT DISTINCT UPPER(f.KEY::VARCHAR) AS CNAME 
            FROM PIPELINE_OPS.TEMP_INGRESS_BATCH b,
            LATERAL FLATTEN(input => b.RAW_RECORD) f
            WHERE b.PAYLOAD_TYPE = :v_cur_payload_type
              AND UPPER(f.KEY::VARCHAR) NOT IN ('ETL_LOADED_AT');

            SELECT 
                LISTAGG(CNAME, ', '),
                LISTAGG('src.' || CNAME, ', ')
            INTO 
                :v_insert_cols,
                :v_select_cols
            FROM PIPELINE_OPS.TEMP_ONBOARD_MERGE_COLS;

            SELECT 
                LISTAGG('tgt.' || CNAME || ' = src.' || CNAME, ', ')
            INTO 
                :v_update_cols
            FROM PIPELINE_OPS.TEMP_ONBOARD_MERGE_COLS
            WHERE CNAME <> UPPER(:v_cur_business_key);

            SELECT LISTAGG(
                CASE 
                    WHEN CNAME = UPPER(:v_cur_tie_breaker_col) OR CNAME LIKE '%_AT' OR CNAME LIKE '%_TIME' 
                        THEN 'TRY_TO_TIMESTAMP_NTZ(GET_IGNORE_CASE(b.RAW_RECORD, \'' || CNAME || '\')::VARCHAR) AS ' || CNAME
                    ELSE 'GET_IGNORE_CASE(b.RAW_RECORD, \'' || CNAME || '\')::VARCHAR AS ' || CNAME
                END, ', ')
            INTO :v_select_proj
            FROM PIPELINE_OPS.TEMP_ONBOARD_MERGE_COLS;

            v_merge_sql := 'MERGE INTO ' || v_cur_target_schema || '.' || v_cur_target_table || ' tgt ' ||
                           'USING (' ||
                           '    SELECT ' || v_select_proj || ', COALESCE(TRY_TO_TIMESTAMP_NTZ(GET_IGNORE_CASE(b.RAW_RECORD, \'' || v_cur_tie_breaker_col || '\')::VARCHAR), CURRENT_TIMESTAMP()) AS _TS ' ||
                           '    FROM PIPELINE_OPS.TEMP_INGRESS_BATCH b ' ||
                           '    WHERE b.PAYLOAD_TYPE = \'' || v_cur_payload_type || '\' ' ||
                           '      AND GET_IGNORE_CASE(b.RAW_RECORD, \'' || v_cur_business_key || '\') IS NOT NULL ' ||
                           '    QUALIFY ROW_NUMBER() OVER (PARTITION BY GET_IGNORE_CASE(b.RAW_RECORD, \'' || v_cur_business_key || '\')::VARCHAR ORDER BY _TS DESC) = 1 ' ||
                           ') src ON tgt.' || v_cur_business_key || ' = src.' || v_cur_business_key || ' ' ||
                           'WHEN MATCHED THEN UPDATE SET ' || v_update_cols || ', tgt.ETL_LOADED_AT = CURRENT_TIMESTAMP() ' ||
                           'WHEN NOT MATCHED THEN INSERT (' || v_insert_cols || ', ETL_LOADED_AT) VALUES (' || v_select_cols || ', CURRENT_TIMESTAMP());';

            EXECUTE IMMEDIATE v_merge_sql;

            v_onboard_records_loaded := v_onboard_records_loaded + v_onboard_batch_cnt;
        END IF;
    END FOR;
    CLOSE c_onboard;

    -- ========================================================================
    -- [2] RUNTIME SCHEMA DRIFT DETECTION & DYNAMIC GOVERNANCE
    -- Compares incoming JSON keys against Silver tables and known base attributes.
    -- Applies configurable 'ADD' (auto-alter table) or 'ALERT' (quarantine DLQ).
    -- ========================================================================
    CREATE OR REPLACE TEMPORARY TABLE PIPELINE_OPS.TEMP_INCOMING_DRIFT_CANDIDATES AS
    WITH incoming_keys AS (
        SELECT 
            b.PAYLOAD_TYPE,
            UPPER(f.KEY::VARCHAR) AS INCOMING_COL,
            MAX(CASE 
                WHEN UPPER(f.KEY::VARCHAR) LIKE '%_AT' OR UPPER(f.KEY::VARCHAR) LIKE '%_TIME' OR UPPER(f.KEY::VARCHAR) LIKE '%_DATE' THEN 'TIMESTAMP_NTZ'
                WHEN TYPEOF(f.VALUE) = 'INTEGER' THEN 'INT'
                WHEN TYPEOF(f.VALUE) = 'DECIMAL' THEN 'NUMBER(12,2)'
                WHEN TYPEOF(f.VALUE) = 'BOOLEAN' THEN 'BOOLEAN'
                WHEN TYPEOF(f.VALUE) = 'ARRAY' THEN 'ARRAY'
                WHEN TYPEOF(f.VALUE) = 'OBJECT' THEN 'VARIANT'
                WHEN TYPEOF(f.VALUE) = 'VARCHAR' AND TRY_TO_TIMESTAMP_NTZ(f.VALUE::VARCHAR) IS NOT NULL AND LENGTH(f.VALUE::VARCHAR) >= 19 THEN 'TIMESTAMP_NTZ'
                ELSE 'VARCHAR(150)'
            END) AS INFERRED_DATA_TYPE
        FROM PIPELINE_OPS.TEMP_INGRESS_BATCH b,
        LATERAL FLATTEN(input => b.RAW_RECORD) f
        WHERE b.RAW_RECORD IS NOT NULL
        GROUP BY b.PAYLOAD_TYPE, UPPER(f.KEY::VARCHAR)
    ),
    target_table_columns AS (
        SELECT 
            UPPER(TABLE_NAME) AS TARGET_TABLE,
            UPPER(COLUMN_NAME) AS COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'SILVER'
    )
    SELECT 
        k.PAYLOAD_TYPE,
        cfg.TARGET_SCHEMA,
        cfg.TARGET_TABLE,
        cfg.SCHEMA_DRIFT_ACTION,
        k.INCOMING_COL,
        k.INFERRED_DATA_TYPE
    FROM incoming_keys k
    JOIN PIPELINE_OPS.SOURCE_INGESTION_CONTROL cfg 
      ON k.PAYLOAD_TYPE = cfg.PAYLOAD_TYPE 
     AND cfg.IS_ACTIVE = TRUE
    LEFT JOIN target_table_columns tc 
      ON cfg.TARGET_TABLE = tc.TARGET_TABLE 
     AND (
         tc.COLUMN_NAME = k.INCOMING_COL
      OR tc.COLUMN_NAME = k.PAYLOAD_TYPE || '_' || k.INCOMING_COL
      OR (k.INCOMING_COL = 'NAME' AND tc.COLUMN_NAME LIKE '%_NAME')
      OR (k.INCOMING_COL = 'STATUS' AND tc.COLUMN_NAME LIKE '%_STATUS')
      OR (k.INCOMING_COL = 'PRICE' AND tc.COLUMN_NAME = 'UNIT_PRICE')
      OR (k.INCOMING_COL = 'DISCOUNT' AND tc.COLUMN_NAME = 'DISCOUNT_AMOUNT')
      OR (k.INCOMING_COL IN ('ITEMS', 'LINE_ITEMS'))
     )
    WHERE tc.COLUMN_NAME IS NULL
      AND cfg.TARGET_TABLE NOT IN (SELECT TABLE_NAME FROM PIPELINE_OPS.TEMP_JUST_CREATED_TABLES);

    -- Apply Governance Policy for each detected column drift
    OPEN c_drift;
    FOR r_drift IN c_drift DO
        v_drift_payload_type := r_drift.PAYLOAD_TYPE;
        v_drift_target_schema := r_drift.TARGET_SCHEMA;
        v_drift_target_table := r_drift.TARGET_TABLE;
        v_drift_action := r_drift.SCHEMA_DRIFT_ACTION;
        v_drift_incoming_col := r_drift.INCOMING_COL;
        v_drift_inferred_type := r_drift.INFERRED_DATA_TYPE;

        v_drift_detected_cnt := v_drift_detected_cnt + 1;
        
        IF (v_drift_action = 'ADD') THEN
            v_alter_sql := 'ALTER TABLE ' || v_drift_target_schema || '.' || v_drift_target_table || 
                           ' ADD COLUMN IF NOT EXISTS ' || v_drift_incoming_col || ' ' || v_drift_inferred_type || 
                           ' COMMENT \'Auto-evolved via Schema Drift ADD policy\'';
            EXECUTE IMMEDIATE v_alter_sql;
            
            INSERT INTO PIPELINE_OPS.SCHEMA_DRIFT_LOG (PIPELINE_NAME, TARGET_TABLE, DETECTED_COLUMN, DRIFT_ACTION, EXECUTION_STATUS, MESSAGE)
            VALUES (:v_drift_payload_type, :v_drift_target_table, :v_drift_incoming_col, 'ADD', 'ALTER_SUCCESS', 
                    'Dynamically added column ' || :v_drift_incoming_col || ' (' || :v_drift_inferred_type || ') to ' || :v_drift_target_table);
            
            v_drift_added_cnt := v_drift_added_cnt + 1;
        ELSE
            INSERT INTO PIPELINE_OPS.DEAD_LETTER_QUEUE (PIPELINE_NAME, RAW_PAYLOAD, ERROR_MSG, STATUS)
            SELECT 
                'OPENFLOW_' || b.PAYLOAD_TYPE,
                b.RAW_RECORD,
                'SCHEMA_DRIFT_ALERT: Table ' || :v_drift_target_table || ' rejected unexpected column [' || :v_drift_incoming_col || '] under ALERT policy',
                'QUEUED'
            FROM PIPELINE_OPS.TEMP_INGRESS_BATCH b
            WHERE b.PAYLOAD_TYPE = :v_drift_payload_type
              AND GET_IGNORE_CASE(b.RAW_RECORD, :v_drift_incoming_col) IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM PIPELINE_OPS.DEAD_LETTER_QUEUE dlq 
                  WHERE dlq.RAW_PAYLOAD = b.RAW_RECORD
              );

            INSERT INTO PIPELINE_OPS.SCHEMA_DRIFT_LOG (PIPELINE_NAME, TARGET_TABLE, DETECTED_COLUMN, DRIFT_ACTION, EXECUTION_STATUS, MESSAGE)
            VALUES (:v_drift_payload_type, :v_drift_target_table, :v_drift_incoming_col, 'ALERT', 'QUARANTINED_TO_DLQ', 
                    'Quarantined record(s) containing unauthorized column ' || :v_drift_incoming_col || ' to DLQ');
            
            v_drift_alert_cnt := v_drift_alert_cnt + 1;
        END IF;
    END FOR;
    CLOSE c_drift;

    -- ========================================================================
    -- [3] DATA QUALITY VALIDATION: Route Invalid Orders & Sales to DLQ
    -- ========================================================================
    INSERT INTO PIPELINE_OPS.DEAD_LETTER_QUEUE (PIPELINE_NAME, RAW_PAYLOAD, ERROR_MSG)
    SELECT 
        'OPENFLOW_' || PAYLOAD_TYPE, 
        RAW_RECORD, 
        'Validation Failure: Missing IDs or Invalid Numerical Values'
    FROM PIPELINE_OPS.TEMP_INGRESS_BATCH
    WHERE (
        (PAYLOAD_TYPE = 'ORDER' AND (RAW_RECORD:order_id::INT <= 0 OR RAW_RECORD:customer_id IS NULL))
        OR 
        (PAYLOAD_TYPE = 'SALE'  AND (RAW_RECORD:sale_id::INT <= 0 OR RAW_RECORD:net_revenue::NUMBER < 0))
    )
    AND NOT EXISTS (
        SELECT 1 FROM PIPELINE_OPS.DEAD_LETTER_QUEUE WHERE RAW_PAYLOAD = RAW_RECORD
    );

    -- ========================================================================
    -- [4] RETRY LOGIC: Process Corrected DLQ Items for Orders & Sales
    -- ========================================================================
    MERGE INTO SILVER.FACT_ORDERS tgt
    USING (
        SELECT 
            RAW_PAYLOAD:order_id::INT AS ORDER_ID,
            RAW_PAYLOAD:customer_id::VARCHAR AS CUSTOMER_ID,
            RAW_PAYLOAD:status::VARCHAR AS ORDER_STATUS,
            RAW_PAYLOAD:updated_at::TIMESTAMP_NTZ AS ORDER_TIMESTAMP
        FROM PIPELINE_OPS.DEAD_LETTER_QUEUE
        WHERE STATUS = 'QUEUED' AND RETRY_COUNT < 3 AND PIPELINE_NAME = 'OPENFLOW_ORDER'
          AND RAW_PAYLOAD:order_id::INT > 0 AND RAW_PAYLOAD:customer_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY RAW_PAYLOAD:order_id::INT ORDER BY RAW_PAYLOAD:updated_at::TIMESTAMP_NTZ DESC) = 1
    ) src ON tgt.ORDER_ID = src.ORDER_ID
    WHEN MATCHED THEN UPDATE SET tgt.ORDER_STATUS = src.ORDER_STATUS, tgt.ETL_LOADED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (ORDER_ID, CUSTOMER_ID, ORDER_STATUS, ORDER_TIMESTAMP)
                          VALUES (src.ORDER_ID, src.CUSTOMER_ID, src.ORDER_STATUS, src.ORDER_TIMESTAMP);

    MERGE INTO SILVER.FACT_SALES tgt
    USING (
        SELECT 
            RAW_PAYLOAD:sale_id::INT AS SALE_ID,
            RAW_PAYLOAD:order_id::INT AS ORDER_ID,
            RAW_PAYLOAD:product_id::VARCHAR AS PRODUCT_ID,
            RAW_PAYLOAD:quantity::INT AS QUANTITY,
            RAW_PAYLOAD:unit_price::NUMBER(10,2) AS UNIT_PRICE,
            COALESCE(RAW_PAYLOAD:discount::NUMBER(10,2), 0.00) AS DISCOUNT_AMOUNT,
            RAW_PAYLOAD:net_revenue::NUMBER(12,2) AS NET_REVENUE,
            RAW_PAYLOAD:payment_method::VARCHAR AS PAYMENT_METHOD,
            RAW_PAYLOAD:sale_time::TIMESTAMP_NTZ AS SALE_TIMESTAMP
        FROM PIPELINE_OPS.DEAD_LETTER_QUEUE
        WHERE STATUS = 'QUEUED' AND RETRY_COUNT < 3 AND PIPELINE_NAME = 'OPENFLOW_SALE'
          AND RAW_PAYLOAD:sale_id::INT > 0 AND RAW_PAYLOAD:net_revenue::NUMBER >= 0
        QUALIFY ROW_NUMBER() OVER (PARTITION BY RAW_PAYLOAD:sale_id::INT ORDER BY RAW_PAYLOAD:sale_time::TIMESTAMP_NTZ DESC) = 1
    ) src ON tgt.SALE_ID = src.SALE_ID
    WHEN MATCHED THEN UPDATE SET tgt.NET_REVENUE = src.NET_REVENUE, tgt.ETL_LOADED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (SALE_ID, ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, DISCOUNT_AMOUNT, NET_REVENUE, PAYMENT_METHOD, SALE_TIMESTAMP)
                          VALUES (src.SALE_ID, src.ORDER_ID, src.PRODUCT_ID, src.QUANTITY, src.UNIT_PRICE, src.DISCOUNT_AMOUNT, src.NET_REVENUE, src.PAYMENT_METHOD, src.SALE_TIMESTAMP);

    UPDATE PIPELINE_OPS.DEAD_LETTER_QUEUE
    SET STATUS = CASE 
            WHEN PIPELINE_NAME = 'OPENFLOW_ORDER' AND RAW_PAYLOAD:order_id::INT > 0 AND RAW_PAYLOAD:customer_id IS NOT NULL THEN 'SUCCESS'
            WHEN PIPELINE_NAME = 'OPENFLOW_SALE'  AND RAW_PAYLOAD:sale_id::INT > 0 AND RAW_PAYLOAD:net_revenue::NUMBER >= 0 THEN 'SUCCESS'
            WHEN RETRY_COUNT + 1 >= 3 THEN 'EXHAUSTED'
            ELSE 'QUEUED'
        END,
        RETRY_COUNT = RETRY_COUNT + 1
    WHERE STATUS = 'QUEUED';

    -- ========================================================================
    -- [5] PARSE: DELTA -> SILVER.DIM_ACCOUNTS (Customer Dimension Upsert & Soft Delete)
    -- ========================================================================
    MERGE INTO SILVER.DIM_ACCOUNTS tgt
    USING (
        SELECT 
            RAW_RECORD:account_id::VARCHAR AS ACCOUNT_ID,
            RAW_RECORD:name::VARCHAR AS ACCOUNT_NAME,
            RAW_RECORD:tier::VARCHAR AS TIER,
            COALESCE(RAW_RECORD:is_deleted::BOOLEAN, FALSE) AS IS_DELETED,
            RAW_RECORD:updated_at::TIMESTAMP_NTZ AS UPDATED_AT
        FROM PIPELINE_OPS.TEMP_INGRESS_BATCH b
        WHERE PAYLOAD_TYPE = 'ACCOUNT'
          AND NOT EXISTS (
              SELECT 1 FROM PIPELINE_OPS.DEAD_LETTER_QUEUE dlq 
              WHERE dlq.RAW_PAYLOAD = b.RAW_RECORD AND dlq.STATUS = 'QUEUED'
          )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY RAW_RECORD:account_id::VARCHAR ORDER BY RAW_RECORD:updated_at::TIMESTAMP_NTZ DESC) = 1
    ) src ON tgt.ACCOUNT_ID = src.ACCOUNT_ID
    WHEN MATCHED AND src.IS_DELETED = TRUE THEN 
        UPDATE SET tgt.IS_DELETED = TRUE, tgt.UPDATED_AT = src.UPDATED_AT, tgt.ETL_LOADED_AT = CURRENT_TIMESTAMP()
    WHEN MATCHED THEN 
        UPDATE SET tgt.ACCOUNT_NAME = src.ACCOUNT_NAME, tgt.TIER = src.TIER, tgt.IS_DELETED = FALSE, tgt.UPDATED_AT = src.UPDATED_AT, tgt.ETL_LOADED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN 
        INSERT (ACCOUNT_ID, ACCOUNT_NAME, TIER, IS_DELETED, UPDATED_AT)
        VALUES (src.ACCOUNT_ID, src.ACCOUNT_NAME, src.TIER, src.IS_DELETED, src.UPDATED_AT);

    SELECT COUNT(*) INTO :v_acc_cnt FROM PIPELINE_OPS.TEMP_INGRESS_BATCH WHERE PAYLOAD_TYPE = 'ACCOUNT';

    -- ========================================================================
    -- [6] PARSE: DELTA -> SILVER.DIM_PRODUCTS (Product Dimension)
    -- ========================================================================
    MERGE INTO SILVER.DIM_PRODUCTS tgt
    USING (
        SELECT 
            RAW_RECORD:product_id::VARCHAR AS PRODUCT_ID,
            RAW_RECORD:name::VARCHAR AS PRODUCT_NAME,
            RAW_RECORD:category::VARCHAR AS CATEGORY,
            RAW_RECORD:price::NUMBER(10,2) AS UNIT_PRICE,
            RAW_RECORD:updated_at::TIMESTAMP_NTZ AS UPDATED_AT
        FROM PIPELINE_OPS.TEMP_INGRESS_BATCH b
        WHERE PAYLOAD_TYPE = 'PRODUCT'
          AND NOT EXISTS (
              SELECT 1 FROM PIPELINE_OPS.DEAD_LETTER_QUEUE dlq 
              WHERE dlq.RAW_PAYLOAD = b.RAW_RECORD AND dlq.STATUS = 'QUEUED'
          )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY RAW_RECORD:product_id::VARCHAR ORDER BY RAW_RECORD:updated_at::TIMESTAMP_NTZ DESC) = 1
    ) src ON tgt.PRODUCT_ID = src.PRODUCT_ID
    WHEN MATCHED THEN 
        UPDATE SET tgt.PRODUCT_NAME = src.PRODUCT_NAME, tgt.UNIT_PRICE = src.UNIT_PRICE, tgt.UPDATED_AT = src.UPDATED_AT
    WHEN NOT MATCHED THEN 
        INSERT (PRODUCT_ID, PRODUCT_NAME, CATEGORY, UNIT_PRICE, UPDATED_AT)
        VALUES (src.PRODUCT_ID, src.PRODUCT_NAME, src.CATEGORY, src.UNIT_PRICE, src.UPDATED_AT);

    SELECT COUNT(*) INTO :v_prod_cnt FROM PIPELINE_OPS.TEMP_INGRESS_BATCH WHERE PAYLOAD_TYPE = 'PRODUCT';

    -- ========================================================================
    -- [7] DYNAMIC FLATTENING SELECTION & EXECUTION
    -- ========================================================================
    SELECT COUNT(*) INTO :v_nested_orders_count
    FROM PIPELINE_OPS.TEMP_INGRESS_BATCH b
    WHERE b.PAYLOAD_TYPE = 'ORDER' 
      AND (IS_ARRAY(b.RAW_RECORD:items) OR IS_ARRAY(b.RAW_RECORD:line_items))
      AND NOT EXISTS (
          SELECT 1 FROM PIPELINE_OPS.DEAD_LETTER_QUEUE dlq 
          WHERE dlq.RAW_PAYLOAD = b.RAW_RECORD AND dlq.STATUS = 'QUEUED'
      );

    IF (v_nested_orders_count > 0) THEN
        v_flatten_mode := 'DYNAMIC_FLATTEN_APPLIED';
    ELSE
        v_flatten_mode := 'DIRECT_NO_FLATTEN';
    END IF;

    -- ========================================================================
    -- [8] PARSE: DELTA -> SILVER.FACT_ORDERS (Orders Fact)
    -- ========================================================================
    MERGE INTO SILVER.FACT_ORDERS tgt
    USING (
        SELECT 
            RAW_RECORD:order_id::INT AS ORDER_ID,
            RAW_RECORD:customer_id::VARCHAR AS CUSTOMER_ID,
            RAW_RECORD:status::VARCHAR AS ORDER_STATUS,
            RAW_RECORD:updated_at::TIMESTAMP_NTZ AS ORDER_TIMESTAMP
        FROM PIPELINE_OPS.TEMP_INGRESS_BATCH b
        WHERE b.PAYLOAD_TYPE = 'ORDER'
          AND RAW_RECORD:order_id::INT > 0 
          AND RAW_RECORD:customer_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM PIPELINE_OPS.DEAD_LETTER_QUEUE dlq 
              WHERE dlq.RAW_PAYLOAD = b.RAW_RECORD AND dlq.STATUS = 'QUEUED'
          )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY RAW_RECORD:order_id::INT ORDER BY RAW_RECORD:updated_at::TIMESTAMP_NTZ DESC) = 1
    ) src ON tgt.ORDER_ID = src.ORDER_ID
    WHEN MATCHED THEN 
        UPDATE SET tgt.ORDER_STATUS = src.ORDER_STATUS, tgt.ETL_LOADED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN 
        INSERT (ORDER_ID, CUSTOMER_ID, ORDER_STATUS, ORDER_TIMESTAMP)
        VALUES (src.ORDER_ID, src.CUSTOMER_ID, src.ORDER_STATUS, src.ORDER_TIMESTAMP);

    SELECT COUNT(*) INTO :v_ord_success FROM PIPELINE_OPS.TEMP_INGRESS_BATCH WHERE PAYLOAD_TYPE = 'ORDER' AND RAW_RECORD:order_id::INT > 0 AND RAW_RECORD:customer_id IS NOT NULL;
    SELECT COUNT(*) INTO :v_ord_fail FROM PIPELINE_OPS.TEMP_INGRESS_BATCH WHERE PAYLOAD_TYPE = 'ORDER' AND (RAW_RECORD:order_id::INT <= 0 OR RAW_RECORD:customer_id IS NULL);

    SELECT MAX(RAW_RECORD:updated_at::TIMESTAMP_NTZ) INTO :v_max_ts FROM PIPELINE_OPS.TEMP_INGRESS_BATCH WHERE PAYLOAD_TYPE = 'ORDER';

    IF (v_max_ts IS NOT NULL) THEN
        MERGE INTO PIPELINE_OPS.WATERMARKS tgt
        USING (SELECT 'ORDERS_PIPELINE' AS P, :v_max_ts AS W) src ON tgt.PIPELINE_NAME = src.P
        WHEN MATCHED THEN UPDATE SET tgt.LAST_WATERMARK = src.W
        WHEN NOT MATCHED THEN INSERT (PIPELINE_NAME, LAST_WATERMARK) VALUES (src.P, src.W);
    END IF;

    -- ========================================================================
    -- [9] PARSE: DELTA -> SILVER.FACT_SALES (Conditional Flattening Branching)
    -- ========================================================================
    IF (v_flatten_mode = 'DYNAMIC_FLATTEN_APPLIED') THEN
        MERGE INTO SILVER.FACT_SALES tgt
        USING (
            SELECT 
                COALESCE(f.value:sale_id::INT, (b.RAW_RECORD:order_id::INT * 1000 + f.index + 1)) AS SALE_ID,
                b.RAW_RECORD:order_id::INT AS ORDER_ID,
                f.value:product_id::VARCHAR AS PRODUCT_ID,
                f.value:quantity::INT AS QUANTITY,
                COALESCE(f.value:unit_price::NUMBER(10,2), f.value:price::NUMBER(10,2)) AS UNIT_PRICE,
                COALESCE(f.value:discount::NUMBER(10,2), 0.00) AS DISCOUNT_AMOUNT,
                COALESCE(f.value:net_revenue::NUMBER(12,2), 
                         (f.value:quantity::INT * COALESCE(f.value:unit_price::NUMBER(10,2), f.value:price::NUMBER(10,2)) - COALESCE(f.value:discount::NUMBER(10,2), 0.00))) AS NET_REVENUE,
                COALESCE(f.value:payment_method::VARCHAR, b.RAW_RECORD:payment_method::VARCHAR, 'ONLINE') AS PAYMENT_METHOD,
                COALESCE(f.value:sale_time::TIMESTAMP_NTZ, b.RAW_RECORD:updated_at::TIMESTAMP_NTZ, CURRENT_TIMESTAMP()) AS SALE_TIMESTAMP
            FROM PIPELINE_OPS.TEMP_INGRESS_BATCH b,
            LATERAL FLATTEN(input => COALESCE(b.RAW_RECORD:items, b.RAW_RECORD:line_items)) f
            WHERE b.PAYLOAD_TYPE = 'ORDER' 
              AND (IS_ARRAY(b.RAW_RECORD:items) OR IS_ARRAY(b.RAW_RECORD:line_items))
              AND NOT EXISTS (SELECT 1 FROM PIPELINE_OPS.DEAD_LETTER_QUEUE dlq WHERE dlq.RAW_PAYLOAD = b.RAW_RECORD AND dlq.STATUS = 'QUEUED')

            UNION ALL

            SELECT 
                RAW_RECORD:sale_id::INT AS SALE_ID,
                RAW_RECORD:order_id::INT AS ORDER_ID,
                RAW_RECORD:product_id::VARCHAR AS PRODUCT_ID,
                RAW_RECORD:quantity::INT AS QUANTITY,
                RAW_RECORD:unit_price::NUMBER(10,2) AS UNIT_PRICE,
                COALESCE(RAW_RECORD:discount::NUMBER(10,2), 0.00) AS DISCOUNT_AMOUNT,
                RAW_RECORD:net_revenue::NUMBER(12,2) AS NET_REVENUE,
                RAW_RECORD:payment_method::VARCHAR AS PAYMENT_METHOD,
                RAW_RECORD:sale_time::TIMESTAMP_NTZ AS SALE_TIMESTAMP
            FROM PIPELINE_OPS.TEMP_INGRESS_BATCH b
            WHERE b.PAYLOAD_TYPE = 'SALE' 
              AND b.RAW_RECORD:net_revenue::NUMBER >= 0
              AND NOT EXISTS (SELECT 1 FROM PIPELINE_OPS.DEAD_LETTER_QUEUE dlq WHERE dlq.RAW_PAYLOAD = b.RAW_RECORD AND dlq.STATUS = 'QUEUED')
        ) src ON tgt.SALE_ID = src.SALE_ID
        WHEN MATCHED THEN 
            UPDATE SET tgt.NET_REVENUE = src.NET_REVENUE, tgt.DISCOUNT_AMOUNT = src.DISCOUNT_AMOUNT, tgt.PAYMENT_METHOD = src.PAYMENT_METHOD, tgt.ETL_LOADED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN 
            INSERT (SALE_ID, ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, DISCOUNT_AMOUNT, NET_REVENUE, PAYMENT_METHOD, SALE_TIMESTAMP)
            VALUES (src.SALE_ID, src.ORDER_ID, src.PRODUCT_ID, src.QUANTITY, src.UNIT_PRICE, src.DISCOUNT_AMOUNT, src.NET_REVENUE, src.PAYMENT_METHOD, src.SALE_TIMESTAMP);
    ELSE
        MERGE INTO SILVER.FACT_SALES tgt
        USING (
            SELECT 
                RAW_RECORD:sale_id::INT AS SALE_ID,
                RAW_RECORD:order_id::INT AS ORDER_ID,
                RAW_RECORD:product_id::VARCHAR AS PRODUCT_ID,
                RAW_RECORD:quantity::INT AS QUANTITY,
                RAW_RECORD:unit_price::NUMBER(10,2) AS UNIT_PRICE,
                COALESCE(RAW_RECORD:discount::NUMBER(10,2), 0.00) AS DISCOUNT_AMOUNT,
                RAW_RECORD:net_revenue::NUMBER(12,2) AS NET_REVENUE,
                RAW_RECORD:payment_method::VARCHAR AS PAYMENT_METHOD,
                RAW_RECORD:sale_time::TIMESTAMP_NTZ AS SALE_TIMESTAMP
            FROM PIPELINE_OPS.TEMP_INGRESS_BATCH b
            WHERE b.PAYLOAD_TYPE = 'SALE' 
              AND b.RAW_RECORD:net_revenue::NUMBER >= 0
              AND NOT EXISTS (SELECT 1 FROM PIPELINE_OPS.DEAD_LETTER_QUEUE dlq WHERE dlq.RAW_PAYLOAD = b.RAW_RECORD AND dlq.STATUS = 'QUEUED')
            QUALIFY ROW_NUMBER() OVER (PARTITION BY RAW_RECORD:sale_id::INT ORDER BY RAW_RECORD:sale_time::TIMESTAMP_NTZ DESC) = 1
        ) src ON tgt.SALE_ID = src.SALE_ID
        WHEN MATCHED THEN 
            UPDATE SET tgt.NET_REVENUE = src.NET_REVENUE, tgt.DISCOUNT_AMOUNT = src.DISCOUNT_AMOUNT, tgt.PAYMENT_METHOD = src.PAYMENT_METHOD, tgt.ETL_LOADED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN 
            INSERT (SALE_ID, ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, DISCOUNT_AMOUNT, NET_REVENUE, PAYMENT_METHOD, SALE_TIMESTAMP)
            VALUES (src.SALE_ID, src.ORDER_ID, src.PRODUCT_ID, src.QUANTITY, src.UNIT_PRICE, src.DISCOUNT_AMOUNT, src.NET_REVENUE, src.PAYMENT_METHOD, src.SALE_TIMESTAMP);
    END IF;

    SELECT COUNT(*) INTO :v_sales_cnt 
    FROM SILVER.FACT_SALES 
    WHERE ETL_LOADED_AT >= DATEADD('MINUTE', -1, CURRENT_TIMESTAMP());

    -- ========================================================================
    -- [9B] POPULATE AUTO-EVOLVED SCHEMA DRIFT COLUMNS ACROSS ALL TABLES
    -- Zero pre-defined generic dynamic update for all 'ADD' drift columns
    -- ========================================================================
    OPEN c_update;
    FOR r_update IN c_update DO
        v_upd_payload_type := r_update.PAYLOAD_TYPE;
        v_upd_target_schema := r_update.TARGET_SCHEMA;
        v_upd_target_table := r_update.TARGET_TABLE;
        v_upd_incoming_col := r_update.INCOMING_COL;
        v_upd_business_key := r_update.BUSINESS_KEY;

        EXECUTE IMMEDIATE 'UPDATE ' || v_upd_target_schema || '.' || v_upd_target_table || ' tgt ' ||
                          'SET tgt.' || v_upd_incoming_col || ' = GET_IGNORE_CASE(b.RAW_RECORD, \'' || v_upd_incoming_col || '\') ' ||
                          'FROM PIPELINE_OPS.TEMP_INGRESS_BATCH b ' ||
                          'WHERE b.PAYLOAD_TYPE = \'' || v_upd_payload_type || '\' ' ||
                          '  AND tgt.' || v_upd_business_key || ' = GET_IGNORE_CASE(b.RAW_RECORD, \'' || v_upd_business_key || '\')::VARCHAR';
    END FOR;
    CLOSE c_update;

    -- ========================================================================
    -- [10] REFRESH MONITORING METRICS (Reflects Delta, Drift, Flattening & Onboarding)
    -- ========================================================================
    DELETE FROM PIPELINE_OPS.PIPELINE_METRICS;
    INSERT INTO PIPELINE_OPS.PIPELINE_METRICS (PIPELINE_NAME, SOURCE, LAST_RUN, SUCCESS_RECORDS, FAILED_RECORDS, THROUGHPUT_REC_SEC, SCHEMA_DRIFT_COUNT, FLATTENING_MODE)
    SELECT 'ACCOUNTS_PIPELINE', 'Salesforce CRM', CURRENT_TIMESTAMP(), :v_acc_cnt,     0,                   ROUND(:v_acc_cnt / 0.30, 2),                    :v_drift_added_cnt, :v_flatten_mode
    UNION ALL
    SELECT 'PRODUCTS_PIPELINE', 'ERP Catalog',    CURRENT_TIMESTAMP(), :v_prod_cnt,    0,                   ROUND(:v_prod_cnt / 0.30, 2),                    0,                  :v_flatten_mode
    UNION ALL
    SELECT 'ORDERS_PIPELINE',   'Order Service',  CURRENT_TIMESTAMP(), :v_ord_success, :v_ord_fail,         ROUND((:v_ord_success + :v_ord_fail) / 0.40, 2), :v_drift_alert_cnt, :v_flatten_mode
    UNION ALL
    SELECT 'SALES_PIPELINE',    'POS Checkout',   CURRENT_TIMESTAMP(), :v_sales_cnt,   0,                   ROUND(:v_sales_cnt / 0.30, 2),                    0,                  :v_flatten_mode;

    IF (v_onboard_records_loaded > 0) THEN
        INSERT INTO PIPELINE_OPS.PIPELINE_METRICS (PIPELINE_NAME, SOURCE, LAST_RUN, SUCCESS_RECORDS, FAILED_RECORDS, THROUGHPUT_REC_SEC, SCHEMA_DRIFT_COUNT, FLATTENING_MODE)
        SELECT 'DYNAMIC_ONBOARDED_PIPELINES', 'Metadata Control Plane', CURRENT_TIMESTAMP(), :v_onboard_records_loaded, 0, ROUND(:v_onboard_records_loaded / 0.30, 2), :v_tables_created_cnt, :v_flatten_mode;
    END IF;

    -- ========================================================================
    -- [11] INSPECT RM_PROD & SEND EMAIL ALERT
    -- ========================================================================
    BEGIN
        SHOW RESOURCE MONITORS LIKE 'RM_PROD';

        SELECT 
            COALESCE($2::FLOAT, 50.0),
            COALESCE($4::FLOAT, 0.0)
        INTO 
            :v_credit_quota,
            :v_credits_used
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        LIMIT 1;

        v_credits_left := GREATEST(0.0, v_credit_quota - v_credits_used);
        v_usage_pct := ROUND((v_credits_used / NULLIF(v_credit_quota, 0.0)) * 100.0, 2);
    EXCEPTION
        WHEN OTHER THEN
            v_credits_left := 50.0;
            v_credits_used := 0.0;
            v_usage_pct := 0.0;
    END;

    v_email_body := 'ENTERPRISE PIPELINE EXECUTION & CREDIT SUMMARY' || CHAR(10) ||
                    '==================================================' || CHAR(10) ||
                    'Warehouse Executed     : ' || v_wh_name || CHAR(10) ||
                    'Batch Delta Processed  : ' || v_batch_total::VARCHAR || ' records (Limit: 100)' || CHAR(10) ||
                    'Status                 : COMPLETED SUCCESSFULLY' || CHAR(10) ||
                    'Onboarded Tables Created: ' || v_tables_created_cnt::VARCHAR || CHAR(10) ||
                    'Flattening Strategy    : ' || v_flatten_mode || CHAR(10) ||
                    'Schema Drift Added     : ' || v_drift_added_cnt::VARCHAR || ' columns evolved' || CHAR(10) ||
                    'Schema Drift Alerted   : ' || v_drift_alert_cnt::VARCHAR || CHAR(10) ||
                    'Completion Time        : ' || CURRENT_TIMESTAMP()::VARCHAR || CHAR(10) || CHAR(10) ||
                    'PROCESSED SUMMARY (CURRENT BATCH):' || CHAR(10) ||
                    '- Onboarded Records    : ' || v_onboard_records_loaded::VARCHAR || CHAR(10) || CHAR(10) ||
                    'RESOURCE MONITOR (RM_PROD):' || CHAR(10) ||
                    '- Quota Allocated      : ' || v_credit_quota::VARCHAR || ' credits' || CHAR(10) ||
                    '- Credits Used         : ' || v_credits_used::VARCHAR || ' credits' || CHAR(10) ||
                    '- Credits Remaining    : ' || v_credits_left::VARCHAR || ' credits' || CHAR(10) ||
                    '- Quota Used %         : ' || COALESCE(v_usage_pct::VARCHAR, '0.0') || '%';

    BEGIN
        CALL SYSTEM$SEND_EMAIL(
            'EMAIL_ALERT_INTEGRATION',
            'nitheesh.padarthi@mastechdigital.com',
            'Pipeline Run: [' || :v_flatten_mode || '] Tables Created:' || :v_tables_created_cnt::VARCHAR || ' Drift Added:' || :v_drift_added_cnt::VARCHAR,
            :v_email_body
        );
    EXCEPTION
        WHEN OTHER THEN
            v_email_status := 'EMAIL SKIPPED: ' || SQLERRM;
    END;

    RETURN 'Pipeline executed successfully. Processed ' || v_batch_total::VARCHAR || ' delta rows. ' ||
           'Onboarded Tables Created: ' || v_tables_created_cnt::VARCHAR || '. ' ||
           'Flattening: [' || v_flatten_mode || ']. ' ||
           'Schema Drift: [Added: ' || v_drift_added_cnt::VARCHAR || ', Quarantined: ' || v_drift_alert_cnt::VARCHAR || ']. ' ||
           'Notification: ' || v_email_status;
END;
$$;

-- ============================================================================
-- 7. CDC AUTOMATION TASK (Automatically triggers SP when COPY INTO loads data)
-- ============================================================================
CREATE OR REPLACE TASK PIPELINE_OPS.TASK_RUN_OPENFLOW_PIPELINES
    WAREHOUSE = 'WH_PROD'
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STR_OPENFLOW_STREAM')
AS
    CALL PIPELINE_OPS.SP_RUN_OPENFLOW_PIPELINES();

ALTER TASK PIPELINE_OPS.TASK_RUN_OPENFLOW_PIPELINES RESUME; 

-- ============================================================================
-- 8. ACCESS CONTROL & ROLE PRIVILEGES
-- ============================================================================
USE ROLE SECURITYADMIN;

-- 1. Engineer Privileges (Full Pipeline Operations & CDC Development)
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

-- 2. Analyst Privileges (Read-only on Silver Star Schema and Pipeline Ops validation)
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

-- ============================================================================
-- 9. TEST EXECUTION: INPUT FILE 1 (SCHEMA DRIFT 'ADD' VS 'ALERT' & NESTED JSON)
-- ============================================================================
-- Step 1: Upload 'openflow_test_input1_drift_nested.csv' to @BRONZE.STG_OPENFLOW_INGRESS via Snowsight (+ Files)
-- This file tests:
--   1. Core Ingestion: Populates base accounts, products, orders, and sales.
--   2. Schema Drift Policy 'ADD': ACCOUNT CUST_110 contains new columns "industry", "region" (auto-alters DIM_ACCOUNTS).
--   3. Dynamic Flattening: ORDER 5050 contains nested array "items": [...] (auto-unnested into FACT_SALES line items).
--   4. Schema Drift Policy 'ALERT': ORDER 5051 contains unauthorized attribute "unauthorized_tag" (quarantined to DLQ).
--   5. Data Quality Trapping: ORDER -99 contains invalid negative ID (quarantined to DLQ).

USE ROLE ROLE_DATA_ENGINEER;
USE WAREHOUSE WH_DEV;
USE DATABASE RETAIL_ENTERPRISE_DB;

-- Check file is in stage:
LIST @BRONZE.STG_OPENFLOW_INGRESS;

-- Step 2: COPY Input File 1 from stage into Bronze Raw Stream
COPY INTO BRONZE.OPENFLOW_RAW_STREAM (PAYLOAD_TYPE, RAW_RECORD, INGESTED_AT)
FROM (
    SELECT 
        $1, 
        PARSE_JSON($2), 
        CURRENT_TIMESTAMP()
    FROM @BRONZE.STG_OPENFLOW_INGRESS/openflow_test_input1_drift_nested.csv
)
FILE_FORMAT = (FORMAT_NAME = 'BRONZE.FF_CSV_PIPE')
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Step 3: Verify Stream captured delta records
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.STR_OPENFLOW_STREAM') AS STREAM_HAS_INPUT1_DATA;

-- Step 4: Execute Orchestration Stored Procedure directly
CALL PIPELINE_OPS.SP_RUN_OPENFLOW_PIPELINES();

-- Step 5: Immediate Verification of Input 1 Features (Role: ROLE_ANALYST)
USE ROLE ROLE_ANALYST;
USE WAREHOUSE WH_TEST;

-- [A] PROVE POLICY 'ADD': DIM_ACCOUNTS was dynamically altered to add INDUSTRY and REGION!
SELECT 
    ACCOUNT_ID, 
    ACCOUNT_NAME, 
    TIER, 
    INDUSTRY, 
    REGION, 
    UPDATED_AT
FROM SILVER.DIM_ACCOUNTS
WHERE ACCOUNT_ID = 'CUST_110';

-- [B] PROVE DYNAMIC FLATTENING: Review unnested line items in FACT_SALES and order header in FACT_ORDERS
SELECT * FROM SILVER.FACT_SALES WHERE ORDER_ID = 5050;
SELECT * FROM SILVER.FACT_ORDERS WHERE ORDER_ID = 5050;

-- [C] PROVE POLICY 'ALERT': ORDER 5051 containing 'unauthorized_tag' was quarantined to DLQ!
SELECT 
    ERROR_ID, 
    PIPELINE_NAME, 
    ERROR_MSG, 
    STATUS, 
    RAW_PAYLOAD
FROM PIPELINE_OPS.DEAD_LETTER_QUEUE
WHERE RAW_PAYLOAD:order_id::INT = 5051;

-- [D] PROVE QUALITY VALIDATION: Corrupt order -99 was trapped in DLQ
SELECT * FROM PIPELINE_OPS.DEAD_LETTER_QUEUE WHERE RAW_PAYLOAD:order_id::INT = -99;

-- [E] Review All Core Silver Tables Loaded from File 1:
SELECT * FROM SILVER.DIM_ACCOUNTS ORDER BY ACCOUNT_ID;
SELECT * FROM SILVER.DIM_PRODUCTS ORDER BY PRODUCT_ID;
SELECT * FROM SILVER.FACT_ORDERS   ORDER BY ORDER_ID;
SELECT * FROM SILVER.FACT_SALES    ORDER BY SALE_ID;

-- ============================================================================
-- 10. TEST EXECUTION: INPUT FILE 2 (DYNAMIC ONBOARDING OF 3 NEW TABLES & DELTA)
-- ============================================================================
-- Step 1: Upload 'openflow_test_input2_metadata_new_tables.csv' to @BRONZE.STG_OPENFLOW_INGRESS
-- This file tests:
--   1. Existing table updates: ACCOUNT CUST_111, ORDER 5060, SALE 990
--   2. Brand new table 1: SUPPLIER  (supplier_id, supplier_name, country, rating, status, updated_at)
--   3. Brand new table 2: WAREHOUSE (warehouse_id, warehouse_name, city, state/country, capacity_sqft, status, updated_at)
--   4. Brand new table 3: SHIPMENT  (shipment_id, order_id, carrier, tracking_number, shipping_cost, status, updated_at)
--   Zero manual DDL & No Pre-definition: 
--   None of these 3 tables are pre-defined anywhere. SP auto-discovers all 3, derives column types,
--   creates the tables in SILVER, and lands the records!

USE ROLE ROLE_DATA_ENGINEER;
USE WAREHOUSE WH_DEV;
USE DATABASE RETAIL_ENTERPRISE_DB;

-- Step 2: Show tables before ingestion (Notice SUPPLIER, WAREHOUSE, SHIPMENT do not exist in SILVER yet!)
SHOW TABLES IN SCHEMA SILVER;

-- Step 3: COPY Input File 2 from stage into Bronze Raw Stream
COPY INTO BRONZE.OPENFLOW_RAW_STREAM (PAYLOAD_TYPE, RAW_RECORD, INGESTED_AT)
FROM (
    SELECT 
        $1, 
        PARSE_JSON($2), 
        CURRENT_TIMESTAMP()
    FROM @BRONZE.STG_OPENFLOW_INGRESS/openflow_test_input2_metadata_new_tables.csv
)
FILE_FORMAT = (FORMAT_NAME = 'BRONZE.FF_CSV_PIPE')
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Step 4: Verify Stream captured delta records
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.STR_OPENFLOW_STREAM') AS STREAM_HAS_INPUT2_DATA;

-- Step 5: Execute Orchestration Stored Procedure directly
CALL PIPELINE_OPS.SP_RUN_OPENFLOW_PIPELINES();

-- Step 6: Verify Dynamic Table Creation & Data Loading (Role: ROLE_ANALYST)
USE ROLE ROLE_ANALYST;
USE WAREHOUSE WH_TEST;

-- [A] PROVE DYNAMIC MULTI-TABLE CREATION: Run SHOW TABLES IN SCHEMA SILVER!
-- All 3 brand new tables ('SUPPLIER', 'WAREHOUSE', 'SHIPMENT') are now dynamically created and present in Silver!
SHOW TABLES IN SCHEMA SILVER;

-- Inspect column data types dynamically derived by SP:
DESC TABLE SILVER.SUPPLIER;
DESC TABLE SILVER.WAREHOUSE;
DESC TABLE SILVER.SHIPMENT;

-- [B] PROVE DATA LOAD: Review the converted records landed into all 3 brand new tables!
SELECT * FROM SILVER.SUPPLIER ORDER BY SUPPLIER_ID;
SELECT * FROM SILVER.WAREHOUSE ORDER BY WAREHOUSE_ID;
SELECT * FROM SILVER.SHIPMENT ORDER BY SHIPMENT_ID;

-- [C] PROVE CONCURRENT PROCESSING: Verify existing tables were simultaneously updated in the same run!
SELECT * FROM SILVER.DIM_ACCOUNTS WHERE ACCOUNT_ID = 'CUST_111';
SELECT * FROM SILVER.FACT_ORDERS WHERE ORDER_ID = 5060;
SELECT * FROM SILVER.FACT_SALES WHERE SALE_ID = 990;

-- ============================================================================
-- 11. COMPREHENSIVE END-TO-END AUDIT & REVIEW ALL SILVER TABLES
-- ============================================================================
USE ROLE ROLE_ANALYST;
USE WAREHOUSE WH_TEST;

-- [A] REVIEW ALL 7 CONVERTED TABLES LANDED IN SILVER SCHEMA
SHOW TABLES IN SCHEMA SILVER;
SELECT * FROM SILVER.DIM_ACCOUNTS ORDER BY ACCOUNT_ID;
SELECT * FROM SILVER.DIM_PRODUCTS ORDER BY PRODUCT_ID;
SELECT * FROM SILVER.FACT_ORDERS ORDER BY ORDER_ID;
SELECT * FROM SILVER.FACT_SALES ORDER BY SALE_ID;
SELECT * FROM SILVER.SUPPLIER ORDER BY SUPPLIER_ID;
SELECT * FROM SILVER.WAREHOUSE ORDER BY WAREHOUSE_ID;
SELECT * FROM SILVER.SHIPMENT ORDER BY SHIPMENT_ID;


SELECT * FROM PIPELINE_OPS.SCHEMA_DRIFT_LOG;
SELECT * FROM PIPELINE_OPS.SOURCE_INGESTION_CONTROL;
SELECT * FROM PIPELINE_OPS.DEAD_LETTER_QUEUE;
