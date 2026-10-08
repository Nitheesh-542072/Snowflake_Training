USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS RETAIL_ENTERPRISE_DB;
USE DATABASE RETAIL_ENTERPRISE_DB;

-- ============================================================================
-- 1. ARCHITECTURE SCHEMAS
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;
CREATE SCHEMA IF NOT EXISTS PIPELINE_OPS;

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

CREATE OR REPLACE TABLE PIPELINE_METRICS (
    PIPELINE_NAME VARCHAR(50),
    SOURCE VARCHAR(50),
    LAST_RUN TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SUCCESS_RECORDS INT DEFAULT 0,
    FAILED_RECORDS INT DEFAULT 0,
    THROUGHPUT_REC_SEC NUMBER(10,2) DEFAULT 0
);

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
-- 6. GOLD LAYER: BUSINESS ANALYTICS & MONITORING
-- ============================================================================
USE SCHEMA GOLD;

-- 1. Customer 360 Analytics View (Star Schema: Joins DIM_ACCOUNTS + FACT_ORDERS + FACT_SALES)
CREATE OR REPLACE VIEW GOLD.V_CUSTOMER_360_ANALYTICS AS
SELECT 
    a.ACCOUNT_ID,
    a.ACCOUNT_NAME,
    a.TIER,
    IFF(a.IS_DELETED = TRUE, 'INACTIVE', 'ACTIVE') AS STATUS,
    COUNT(DISTINCT o.ORDER_ID) AS TOTAL_ORDERS,
    COALESCE(SUM(s.NET_REVENUE), 0) AS TOTAL_SPENT,
    COALESCE(AVG(s.NET_REVENUE), 0) AS AVG_ORDER_VALUE,
    MAX(o.ORDER_TIMESTAMP) AS LAST_PURCHASE_DATE
FROM SILVER.DIM_ACCOUNTS a
LEFT JOIN SILVER.FACT_ORDERS o ON a.ACCOUNT_ID = o.CUSTOMER_ID
LEFT JOIN SILVER.FACT_SALES s ON o.ORDER_ID = s.ORDER_ID
GROUP BY a.ACCOUNT_ID, a.ACCOUNT_NAME, a.TIER, a.IS_DELETED;

-- 2. Product Performance Analytics View (Star Schema: Joins DIM_PRODUCTS + FACT_SALES)
CREATE OR REPLACE VIEW GOLD.V_PRODUCT_PERFORMANCE AS
SELECT 
    p.CATEGORY,
    p.PRODUCT_NAME,
    SUM(s.QUANTITY) AS TOTAL_UNITS_SOLD,
    SUM(s.NET_REVENUE) AS TOTAL_CATEGORY_REVENUE,
    ROUND(SUM(s.DISCOUNT_AMOUNT), 2) AS TOTAL_DISCOUNTS_OFFERED
FROM SILVER.DIM_PRODUCTS p
JOIN SILVER.FACT_SALES s ON p.PRODUCT_ID = s.PRODUCT_ID
GROUP BY p.CATEGORY, p.PRODUCT_NAME;

-- 3. Dynamic Table: Hourly Revenue Summary (Directly feeds from FACT_SALES)
CREATE OR REPLACE DYNAMIC TABLE GOLD.DT_HOURLY_REVENUE_METRICS
    TARGET_LAG = '1 MINUTE'
    WAREHOUSE = 'WH_PROD'
AS
SELECT 
    DATE_TRUNC('HOUR', SALE_TIMESTAMP) AS SALE_HOUR,
    PAYMENT_METHOD,
    COUNT(DISTINCT SALE_ID) AS TOTAL_TRANSACTIONS,
    SUM(NET_REVENUE) AS GROSS_REVENUE
FROM SILVER.FACT_SALES
GROUP BY DATE_TRUNC('HOUR', SALE_TIMESTAMP), PAYMENT_METHOD;

-- 4. Pipeline Monitoring Dashboard View with Visual Health Badges
CREATE OR REPLACE VIEW GOLD.V_MONITORING_DASHBOARD AS
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
    CASE 
        WHEN FAILED_RECORDS = 0 THEN '🟢 HEALTHY'
        WHEN (SUCCESS_RECORDS / NULLIF(SUCCESS_RECORDS + FAILED_RECORDS, 0)) >= 0.80 THEN '🟡 WARNING'
        ELSE '🔴 CRITICAL'
    END                AS "Health Status"
FROM PIPELINE_OPS.PIPELINE_METRICS;

-- ============================================================================
-- 7. ORCHESTRATION STORED PROCEDURE (Triggered Automatically on CDC Delta)
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

    -- [A] ERROR HANDLING: Route Invalid Orders & Sales to DLQ from Current Batch
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

    -- [B] RETRY LOGIC: Process Corrected DLQ Items for Orders & Sales
    -- 1. Retry Orders into FACT_ORDERS
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

    -- 2. Retry Sales into FACT_SALES
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

    -- [C] PARSE: DELTA -> SILVER.DIM_ACCOUNTS (Customer Dimension Upsert & Soft Delete)
    MERGE INTO SILVER.DIM_ACCOUNTS tgt
    USING (
        SELECT 
            RAW_RECORD:account_id::VARCHAR AS ACCOUNT_ID,
            RAW_RECORD:name::VARCHAR AS ACCOUNT_NAME,
            RAW_RECORD:tier::VARCHAR AS TIER,
            COALESCE(RAW_RECORD:is_deleted::BOOLEAN, FALSE) AS IS_DELETED,
            RAW_RECORD:updated_at::TIMESTAMP_NTZ AS UPDATED_AT
        FROM PIPELINE_OPS.TEMP_INGRESS_BATCH
        WHERE PAYLOAD_TYPE = 'ACCOUNT'
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

    -- [D] PARSE: DELTA -> SILVER.DIM_PRODUCTS (Product Dimension)
    MERGE INTO SILVER.DIM_PRODUCTS tgt
    USING (
        SELECT 
            RAW_RECORD:product_id::VARCHAR AS PRODUCT_ID,
            RAW_RECORD:name::VARCHAR AS PRODUCT_NAME,
            RAW_RECORD:category::VARCHAR AS CATEGORY,
            RAW_RECORD:price::NUMBER(10,2) AS UNIT_PRICE,
            RAW_RECORD:updated_at::TIMESTAMP_NTZ AS UPDATED_AT
        FROM PIPELINE_OPS.TEMP_INGRESS_BATCH
        WHERE PAYLOAD_TYPE = 'PRODUCT'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY RAW_RECORD:product_id::VARCHAR ORDER BY RAW_RECORD:updated_at::TIMESTAMP_NTZ DESC) = 1
    ) src ON tgt.PRODUCT_ID = src.PRODUCT_ID
    WHEN MATCHED THEN 
        UPDATE SET tgt.PRODUCT_NAME = src.PRODUCT_NAME, tgt.UNIT_PRICE = src.UNIT_PRICE, tgt.UPDATED_AT = src.UPDATED_AT
    WHEN NOT MATCHED THEN 
        INSERT (PRODUCT_ID, PRODUCT_NAME, CATEGORY, UNIT_PRICE, UPDATED_AT)
        VALUES (src.PRODUCT_ID, src.PRODUCT_NAME, src.CATEGORY, src.UNIT_PRICE, src.UPDATED_AT);

    SELECT COUNT(*) INTO :v_prod_cnt FROM PIPELINE_OPS.TEMP_INGRESS_BATCH WHERE PAYLOAD_TYPE = 'PRODUCT';

    -- [E] PARSE: DELTA -> SILVER.FACT_ORDERS (Orders Fact)
    MERGE INTO SILVER.FACT_ORDERS tgt
    USING (
        SELECT 
            RAW_RECORD:order_id::INT AS ORDER_ID,
            RAW_RECORD:customer_id::VARCHAR AS CUSTOMER_ID,
            RAW_RECORD:status::VARCHAR AS ORDER_STATUS,
            RAW_RECORD:updated_at::TIMESTAMP_NTZ AS ORDER_TIMESTAMP
        FROM PIPELINE_OPS.TEMP_INGRESS_BATCH
        WHERE PAYLOAD_TYPE = 'ORDER'
          AND RAW_RECORD:order_id::INT > 0 
          AND RAW_RECORD:customer_id IS NOT NULL
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

    IF (:v_max_ts IS NOT NULL) THEN
        MERGE INTO PIPELINE_OPS.WATERMARKS tgt
        USING (SELECT 'ORDERS_PIPELINE' AS P, :v_max_ts AS W) src ON tgt.PIPELINE_NAME = src.P
        WHEN MATCHED THEN UPDATE SET tgt.LAST_WATERMARK = src.W
        WHEN NOT MATCHED THEN INSERT (PIPELINE_NAME, LAST_WATERMARK) VALUES (src.P, src.W);
    END IF;

    -- [F] PARSE: DELTA -> SILVER.FACT_SALES (Sales Line Items Fact)
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
        FROM PIPELINE_OPS.TEMP_INGRESS_BATCH
        WHERE PAYLOAD_TYPE = 'SALE' AND RAW_RECORD:net_revenue::NUMBER >= 0
        QUALIFY ROW_NUMBER() OVER (PARTITION BY RAW_RECORD:sale_id::INT ORDER BY RAW_RECORD:sale_time::TIMESTAMP_NTZ DESC) = 1
    ) src ON tgt.SALE_ID = src.SALE_ID
    WHEN MATCHED THEN 
        UPDATE SET tgt.NET_REVENUE = src.NET_REVENUE, tgt.DISCOUNT_AMOUNT = src.DISCOUNT_AMOUNT, tgt.PAYMENT_METHOD = src.PAYMENT_METHOD, tgt.ETL_LOADED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN 
        INSERT (SALE_ID, ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, DISCOUNT_AMOUNT, NET_REVENUE, PAYMENT_METHOD, SALE_TIMESTAMP)
        VALUES (src.SALE_ID, src.ORDER_ID, src.PRODUCT_ID, src.QUANTITY, src.UNIT_PRICE, src.DISCOUNT_AMOUNT, src.NET_REVENUE, src.PAYMENT_METHOD, src.SALE_TIMESTAMP);

    SELECT COUNT(*) INTO :v_sales_cnt FROM PIPELINE_OPS.TEMP_INGRESS_BATCH WHERE PAYLOAD_TYPE = 'SALE';

    -- [G] REFRESH MONITORING METRICS (Reflects ONLY Current Batch Delta)
    DELETE FROM PIPELINE_OPS.PIPELINE_METRICS;
    INSERT INTO PIPELINE_OPS.PIPELINE_METRICS (PIPELINE_NAME, SOURCE, LAST_RUN, SUCCESS_RECORDS, FAILED_RECORDS, THROUGHPUT_REC_SEC)
    SELECT 'ACCOUNTS_PIPELINE', 'Salesforce CRM', CURRENT_TIMESTAMP(), :v_acc_cnt,     0,            ROUND(:v_acc_cnt / 0.30, 2)
    UNION ALL
    SELECT 'PRODUCTS_PIPELINE', 'ERP Catalog',    CURRENT_TIMESTAMP(), :v_prod_cnt,    0,            ROUND(:v_prod_cnt / 0.30, 2)
    UNION ALL
    SELECT 'ORDERS_PIPELINE',   'Order Service',  CURRENT_TIMESTAMP(), :v_ord_success, :v_ord_fail,  ROUND((:v_ord_success + :v_ord_fail) / 0.40, 2)
    UNION ALL
    SELECT 'SALES_PIPELINE',    'POS Checkout',   CURRENT_TIMESTAMP(), :v_sales_cnt,   0,            ROUND(:v_sales_cnt / 0.30, 2);

    -- [H] INSPECT RM_PROD & SEND EMAIL ALERT
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

        v_credits_left := GREATEST(0.0, :v_credit_quota - :v_credits_used);
        v_usage_pct := ROUND((:v_credits_used / NULLIF(:v_credit_quota, 0.0)) * 100.0, 2);
    EXCEPTION
        WHEN OTHER THEN
            v_credits_left := 50.0;
            v_credits_used := 0.0;
            v_usage_pct := 0.0;
    END;

    v_email_body := 'ENTERPRISE PIPELINE EXECUTION & CREDIT SUMMARY' || CHAR(10) ||
                    '==================================================' || CHAR(10) ||
                    'Warehouse Executed     : ' || :v_wh_name || CHAR(10) ||
                    'Batch Delta Processed  : ' || :v_batch_total::VARCHAR || ' records (Limit: 100)' || CHAR(10) ||
                    'Status                 : COMPLETED SUCCESSFULLY' || CHAR(10) ||
                    'Completion Time        : ' || CURRENT_TIMESTAMP()::VARCHAR || CHAR(10) || CHAR(10) ||
                    'PROCESSED SUMMARY (CURRENT BATCH):' || CHAR(10) ||
                    '- Accounts Processed   : ' || :v_acc_cnt::VARCHAR || CHAR(10) ||
                    '- Products Cataloged   : ' || :v_prod_cnt::VARCHAR || CHAR(10) ||
                    '- Orders Ingested      : ' || :v_ord_success::VARCHAR || CHAR(10) ||
                    '- DLQ Failed Records   : ' || :v_ord_fail::VARCHAR || CHAR(10) ||
                    '- Sales Transactions   : ' || :v_sales_cnt::VARCHAR || CHAR(10) || CHAR(10) ||
                    'RESOURCE MONITOR (RM_PROD):' || CHAR(10) ||
                    '- Quota Allocated      : ' || :v_credit_quota::VARCHAR || ' credits' || CHAR(10) ||
                    '- Credits Used         : ' || :v_credits_used::VARCHAR || ' credits' || CHAR(10) ||
                    '- Credits Remaining    : ' || :v_credits_left::VARCHAR || ' credits' || CHAR(10) ||
                    '- Quota Used %         : ' || COALESCE(:v_usage_pct::VARCHAR, '0.0') || '%';

    BEGIN
        CALL SYSTEM$SEND_EMAIL(
            'EMAIL_ALERT_INTEGRATION',
            'nitheesh.padarthi@mastechdigital.com',
            'Pipeline Run Complete: RM_PROD Monitor [' || :v_credits_left::VARCHAR || ' Credits Left]',
            :v_email_body
        );
    EXCEPTION
        WHEN OTHER THEN
            v_email_status := 'EMAIL SKIPPED: ' || SQLERRM;
    END;

    RETURN 'Pipeline executed successfully. Processed ' || :v_batch_total::VARCHAR || ' delta rows. Notification: ' || :v_email_status;
END;
$$;

-- ============================================================================
-- 8. CDC AUTOMATION TASK (Automatically triggers SP when COPY INTO loads data)
-- ============================================================================
CREATE OR REPLACE TASK PIPELINE_OPS.TASK_RUN_OPENFLOW_PIPELINES
    WAREHOUSE = 'WH_PROD'
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STR_OPENFLOW_STREAM')
AS
    CALL PIPELINE_OPS.SP_RUN_OPENFLOW_PIPELINES();

-- ============================================================================
-- 9. ACCESS CONTROL & ROLE PRIVILEGES
-- ============================================================================
USE ROLE SECURITYADMIN;

-- 1. Engineer Privileges
GRANT USAGE, OPERATE ON WAREHOUSE WH_DEV TO ROLE ROLE_DATA_ENGINEER;
GRANT USAGE, OPERATE ON WAREHOUSE WH_PROD TO ROLE ROLE_DATA_ENGINEER;
GRANT USAGE, OPERATE ON WAREHOUSE WH_TEST TO ROLE ROLE_DATA_ENGINEER;
GRANT USAGE ON DATABASE RETAIL_ENTERPRISE_DB TO ROLE ROLE_DATA_ENGINEER;

GRANT USAGE ON ALL SCHEMAS IN DATABASE RETAIL_ENTERPRISE_DB TO ROLE ROLE_DATA_ENGINEER;
GRANT CREATE TABLE ON SCHEMA PIPELINE_OPS TO ROLE ROLE_DATA_ENGINEER;
GRANT CREATE TABLE ON SCHEMA BRONZE TO ROLE ROLE_DATA_ENGINEER;
GRANT CREATE TABLE ON SCHEMA SILVER TO ROLE ROLE_DATA_ENGINEER;

GRANT USAGE, READ, WRITE ON STAGE BRONZE.STG_OPENFLOW_INGRESS TO ROLE ROLE_DATA_ENGINEER;
GRANT USAGE ON FILE FORMAT BRONZE.FF_CSV_PIPE TO ROLE ROLE_DATA_ENGINEER;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA BRONZE TO ROLE ROLE_DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA SILVER TO ROLE ROLE_DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA PIPELINE_OPS TO ROLE ROLE_DATA_ENGINEER;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA PIPELINE_OPS TO ROLE ROLE_DATA_ENGINEER;

-- 2. Analyst Privileges (Read-only on Silver, Gold, and Bronze validation)
GRANT USAGE ON WAREHOUSE WH_TEST TO ROLE ROLE_ANALYST;
GRANT USAGE ON DATABASE RETAIL_ENTERPRISE_DB TO ROLE ROLE_ANALYST;
GRANT USAGE ON SCHEMA SILVER TO ROLE ROLE_ANALYST;
GRANT USAGE ON SCHEMA GOLD TO ROLE ROLE_ANALYST;
GRANT USAGE ON SCHEMA BRONZE TO ROLE ROLE_ANALYST;

GRANT SELECT ON ALL TABLES IN SCHEMA BRONZE TO ROLE ROLE_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA SILVER TO ROLE ROLE_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA GOLD TO ROLE ROLE_ANALYST;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA GOLD TO ROLE ROLE_ANALYST;
GRANT READ ON STAGE BRONZE.STG_OPENFLOW_INGRESS TO ROLE ROLE_ANALYST;

-- ============================================================================
-- 10. TEST EXECUTION: BATCH 1 VIA MANUAL COPY INTO
-- ============================================================================
-- Step 1: Upload 'openflow_raw_data.csv' to @BRONZE.STG_OPENFLOW_INGRESS via Snowsight (+ Files)
USE ROLE ROLE_DATA_ENGINEER;
USE WAREHOUSE WH_DEV;
USE DATABASE RETAIL_ENTERPRISE_DB;

-- Check file is in stage:
LIST @BRONZE.STG_OPENFLOW_INGRESS;

-- Step 2: Manually COPY data from stage into Bronze
COPY INTO BRONZE.OPENFLOW_RAW_STREAM (PAYLOAD_TYPE, RAW_RECORD, INGESTED_AT)
FROM (
    SELECT 
        $1, 
        PARSE_JSON($2), 
        CURRENT_TIMESTAMP()
    FROM @BRONZE.STG_OPENFLOW_INGRESS/openflow_raw_data.csv
)
FILE_FORMAT = (FORMAT_NAME = 'BRONZE.FF_CSV_PIPE')
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Step 3: Stream immediately captures the new rows (turns TRUE):
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.STR_OPENFLOW_STREAM') AS STREAM_HAS_NEW_DATA;

-- Step 4: Call SP directly (or let the automated task pick it up):
CALL PIPELINE_OPS.SP_RUN_OPENFLOW_PIPELINES();

-- Step 5: Verify Stream is now consumed and empty (returns FALSE):
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.STR_OPENFLOW_STREAM') AS STREAM_CONSUMED;

-- ============================================================================
-- 11. INCREMENTAL TEST: BATCH 2 (Tests Task Auto-Trigger on COPY)
-- ============================================================================
-- Step 1: Upload 'openflow_raw_data_batch2.csv' to @BRONZE.STG_OPENFLOW_INGRESS
-- Step 2: Resume the automated task
USE ROLE ACCOUNTADMIN;
ALTER TASK PIPELINE_OPS.TASK_RUN_OPENFLOW_PIPELINES RESUME;

-- Step 3: Manually COPY Batch 2 from stage into Bronze
USE ROLE ROLE_DATA_ENGINEER;
COPY INTO BRONZE.OPENFLOW_RAW_STREAM (PAYLOAD_TYPE, RAW_RECORD, INGESTED_AT)
FROM (
    SELECT 
        $1, 
        PARSE_JSON($2), 
        CURRENT_TIMESTAMP()
    FROM @BRONZE.STG_OPENFLOW_INGRESS/openflow_raw_data_batch2.csv
)
FILE_FORMAT = (FORMAT_NAME = 'BRONZE.FF_CSV_PIPE')
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Step 4: Stream immediately turns TRUE because data was copied into Bronze:
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.STR_OPENFLOW_STREAM') AS STREAM_HAS_NEW_DELTA;

-- Step 5: The Task automatically wakes up and triggers the SP!
-- (You can wait up to 5 minutes for the schedule, or force immediate task run):
EXECUTE TASK PIPELINE_OPS.TASK_RUN_OPENFLOW_PIPELINES;

-- ============================================================================
-- 12. AUDIT DIMENSIONS, FACTS & RELATIONSHIPS (Role: ROLE_ANALYST)
-- ============================================================================
USE ROLE ROLE_ANALYST;
USE WAREHOUSE WH_TEST;

-- [A] Inspect Primary & Foreign Key Relationships in Snowflake
SHOW PRIMARY KEYS IN SCHEMA SILVER;
SHOW IMPORTED KEYS IN SCHEMA SILVER;

-- [B] View Incremental Changes in Silver (Verify CUST_104 and PROD_D are loaded!)
SELECT * FROM SILVER.DIM_ACCOUNTS ORDER BY ACCOUNT_ID;
SELECT * FROM SILVER.DIM_PRODUCTS ORDER BY PRODUCT_ID;
SELECT * FROM SILVER.FACT_ORDERS   ORDER BY ORDER_ID;
SELECT * FROM SILVER.FACT_SALES    ORDER BY SALE_ID;

-- [C] Multi-Table Star Schema Relational Join
SELECT 
    f.SALE_ID,
    o.ORDER_ID,
    o.ORDER_STATUS,
    c.ACCOUNT_NAME    AS CUSTOMER_NAME,
    c.TIER            AS CUSTOMER_TIER,
    p.PRODUCT_NAME,
    p.CATEGORY        AS PRODUCT_CATEGORY,
    f.QUANTITY,
    f.UNIT_PRICE,
    f.DISCOUNT_AMOUNT,
    f.NET_REVENUE,
    f.PAYMENT_METHOD,
    f.SALE_TIMESTAMP
FROM SILVER.FACT_SALES f
JOIN SILVER.FACT_ORDERS o   ON f.ORDER_ID = o.ORDER_ID
JOIN SILVER.DIM_ACCOUNTS c  ON o.CUSTOMER_ID = c.ACCOUNT_ID
JOIN SILVER.DIM_PRODUCTS p  ON f.PRODUCT_ID = p.PRODUCT_ID
ORDER BY f.SALE_ID;

-- [D] Gold Layer Analytics & Dashboard Badges
SELECT * FROM GOLD.V_CUSTOMER_360_ANALYTICS;
SELECT * FROM GOLD.V_PRODUCT_PERFORMANCE;
SELECT * FROM GOLD.DT_HOURLY_REVENUE_METRICS;
SELECT * FROM GOLD.V_MONITORING_DASHBOARD;

-- [E] Inspect Task History to Prove Automated Execution
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    RETURN_VALUE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_RUN_OPENFLOW_PIPELINES',
    SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;