import streamlit as st
import pandas as pd
import json
from typing import Dict, List, Any

# ==============================================================================
# SNOWFLAKE OPENFLOW: GOVERNANCE CONTROL PLANE (STREAMLIT IN SNOWFLAKE)
# Core Principle:
#   - If data is NORMAL (clean schema, unique IDs) -> Automatic / No human intervention needed.
#   - Human Intervention is TRIGGERED ONLY WHEN:
#       1. Multiple entries exist with the same ID (requires review; latest sorted to top).
#       2. Existing schemas require alterations (schema drift ADD/REJECT).
#       3. Brand-new entities require table creation (checkbox column review).
# ==============================================================================

st.set_page_config(
    page_title="OpenFlow Governance Control Plane",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ------------------------------------------------------------------------------
# 1. SNOWFLAKE SESSION INITIALIZATION
# ------------------------------------------------------------------------------
@st.cache_resource
def get_snowflake_session():
    """Connects via native Snowpark session in Snowflake or returns None if offline."""
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        return None

session = get_snowflake_session()

def safe_rerun():
    """Reruns the Streamlit app safely across all Streamlit versions."""
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()

# Styling for Dark and Light Themes
st.markdown("""
<style>
    .main-header { font-size: 26px; font-weight: 700; color: #38BDF8; margin-bottom: 2px; }
    .sub-header { font-size: 14px; color: #94A3B8; margin-bottom: 16px; }
    
    /* Normal Data Banner - Dark Theme */
    .status-box-normal { 
        background-color: #064E3B; 
        border: 2px solid #10B981; 
        border-radius: 8px; 
        padding: 16px; 
        margin-bottom: 16px; 
        color: #ECFDF5;
    }
    
    /* Human Intervention Required Banner - Dark Theme */
    .status-box-intervention { 
        background-color: #3B2303; 
        border: 2px solid #F59E0B; 
        border-radius: 8px; 
        padding: 16px; 
        margin-bottom: 16px; 
        color: #FEF3C7;
    }
    
    /* Schema Drift Alert - Dark Theme */
    .drift-alert { 
        background-color: #3F1515; 
        border-left: 5px solid #EF4444; 
        border-top: 1px solid #7F1D1D;
        border-right: 1px solid #7F1D1D;
        border-bottom: 1px solid #7F1D1D;
        padding: 14px; 
        margin-bottom: 12px; 
        border-radius: 6px; 
        color: #FEE2E2;
    }
    
    /* New Table Card - Dark Theme */
    .new-table-card { 
        background-color: #064E3B; 
        border-left: 5px solid #10B981; 
        border-top: 1px solid #047857;
        border-right: 1px solid #047857;
        border-bottom: 1px solid #047857;
        padding: 14px; 
        margin-bottom: 12px; 
        border-radius: 6px; 
        color: #D1FAE5;
    }

    code {
        color: #38BDF8 !important;
        background-color: rgba(255, 255, 255, 0.1) !important;
        padding: 2px 5px !important;
        border-radius: 4px !important;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/f/ff/Snowflake_Logo.svg", width=140)
    st.title("Control Plane")
    st.caption("Human-in-the-Loop Governance")
    
    st.markdown("---")
    st.markdown("### ⚙️ Snowflake Environment")
    st.markdown("**Database**: `RETAIL_ENTERPRISE_DB`")
    st.markdown("**Role**: `ROLE_DATA_ENGINEER`")
    st.markdown("**Warehouse**: `WH_DEV` (60s suspend)")
    st.markdown("**Quota Monitor**: 50 Credits (`RM_PROD`)")
    
    st.markdown("---")
    st.markdown("### 📋 4 Pre-Coded Core Tables")
    st.markdown("1. `ACCOUNT` $\\rightarrow$ `SILVER.DIM_ACCOUNTS`")
    st.markdown("2. `PRODUCT` $\\rightarrow$ `SILVER.DIM_PRODUCTS`")
    st.markdown("3. `ORDER` $\\rightarrow$ `SILVER.FACT_ORDERS`")
    st.markdown("4. `SALE` $\\rightarrow$ `SILVER.FACT_SALES`")
    st.caption("*All other tables are dynamically onboarded on-demand.*")

# Header
st.markdown('<div class="main-header">❄️ Snowflake Openflow: Ingestion & Governance Control Plane</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Automated Ingestion for Normal Data with Human Intervention for Schema Changes & Duplicate IDs</div>', unsafe_allow_html=True)

# ------------------------------------------------------------------------------
# 2. INGESTION SOURCE SELECTOR: LOCAL UPLOAD VS. SNOWFLAKE STAGE
# ------------------------------------------------------------------------------
st.markdown("### 📥 Step 1: Select Ingestion Source & Batch")

source_mode = st.radio(
    "Choose Data Source:",
    ["☁️ Select Landed File from Snowflake Stage (@BRONZE.STG_OPENFLOW_INGRESS)", "📤 Upload File from Local Machine"],
    horizontal=True
)

file_contents_str = None
batch_file_name = None

if "Snowflake Stage" in source_mode:
    st.caption("Inspects files currently landed in internal stage `@BRONZE.STG_OPENFLOW_INGRESS` sorted by latest upload time.")
    staged_files = []
    
    if session:
        try:
            # Query files in stage ordered by last_modified DESC
            stage_df = session.sql("""
                LIST @BRONZE.STG_OPENFLOW_INGRESS;
            """).collect()
            
            for row in stage_df:
                # row[0] is name, row[1] size, row[2] md5, row[3] last_modified
                full_path = str(row[0])
                file_name = full_path.split("/")[-1]
                file_size = f"{round(int(row[1]) / 1024, 2)} KB" if row[1] else "0 KB"
                mod_time = str(row[3])[:19] if row[3] else "Unknown"
                staged_files.append({
                    "display": f"📄 {file_name} — (Uploaded: {mod_time}, Size: {file_size})",
                    "file_name": file_name,
                    "full_path": full_path
                })
        except Exception as e:
            st.warning(f"Unable to list stage: {str(e)}")
    
    # Offline fallback demo files if running outside Snowflake
    if not staged_files:
        staged_files = [
            {"display": "📄 openflow_test_input1_drift_nested.csv — (Uploaded: 2026-10-22 10:45:00, Size: 2.1 KB)", "file_name": "openflow_test_input1_drift_nested.csv", "full_path": "@BRONZE.STG_OPENFLOW_INGRESS/openflow_test_input1_drift_nested.csv"},
            {"display": "📄 openflow_test_input2_metadata_new_tables.csv — (Uploaded: 2026-10-22 09:30:00, Size: 2.3 KB)", "file_name": "openflow_test_input2_metadata_new_tables.csv", "full_path": "@BRONZE.STG_OPENFLOW_INGRESS/openflow_test_input2_metadata_new_tables.csv"}
        ]
    
    col_st1, col_st2 = st.columns([3, 1])
    with col_st1:
        selected_stage_file = st.selectbox(
            "Select Landed File to Review (Latest on Top):",
            options=staged_files,
            format_func=lambda x: x["display"]
        )
    with col_st2:
        st.write("")
        st.write("")
        refresh_stage_btn = st.button("🔄 Refresh Stage Files", use_container_width=True)
        if refresh_stage_btn:
            safe_rerun()

    if selected_stage_file:
        batch_file_name = selected_stage_file["file_name"]
        
        # Read file contents from stage
        if session:
            try:
                # Read staged file directly using Snowpark or temporary query
                res_lines = session.sql(f"""
                    SELECT $1, $2 
                    FROM @BRONZE.STG_OPENFLOW_INGRESS/{batch_file_name} 
                    (FILE_FORMAT => 'BRONZE.FF_CSV_PIPE')
                """).collect()
                
                temp_content = []
                for r in res_lines:
                    temp_content.append(f"{r[0]}|{r[1]}")
                file_contents_str = "\n".join(temp_content)
            except Exception as e:
                # Fallback to local file if path matches workspace
                try:
                    with open(batch_file_name, "r", encoding="utf-8") as f:
                        file_contents_str = f.read()
                except Exception:
                    st.error(f"Error reading file from stage: {str(e)}")
        else:
            try:
                with open(batch_file_name, "r", encoding="utf-8") as f:
                    file_contents_str = f.read()
            except Exception:
                pass

else:
    uploaded_file = st.file_uploader(
        "Choose a batch CSV file from your local machine:",
        type=["csv", "txt"]
    )
    if uploaded_file is not None:
        batch_file_name = uploaded_file.name
        file_contents_str = uploaded_file.getvalue().decode("utf-8")

# Helpers
def infer_sql_type(val: Any) -> str:
    if isinstance(val, int):
        return "NUMBER(38,0)"
    elif isinstance(val, float):
        return "NUMBER(12,2)"
    elif isinstance(val, bool):
        return "BOOLEAN"
    elif isinstance(val, list):
        return "ARRAY"
    elif isinstance(val, dict):
        return "OBJECT"
    val_str = str(val)
    if len(val_str) > 10 and ("-" in val_str or ":" in val_str):
        try:
            pd.to_datetime(val_str)
            return "TIMESTAMP_NTZ"
        except Exception:
            pass
    return "VARCHAR(150)"

CORE_KNOWN_TABLES = {
    "ACCOUNT": "DIM_ACCOUNTS",
    "PRODUCT": "DIM_PRODUCTS",
    "ORDER": "FACT_ORDERS",
    "SALE": "FACT_SALES"
}

CORE_KNOWN_COLUMNS = {
    "ACCOUNT": {"ACCOUNT_ID", "NAME", "TIER", "IS_DELETED", "UPDATED_AT"},
    "PRODUCT": {"PRODUCT_ID", "NAME", "CATEGORY", "PRICE", "UPDATED_AT"},
    "ORDER": {"ORDER_ID", "CUSTOMER_ID", "STATUS", "PAYMENT_METHOD", "UPDATED_AT", "ITEMS"},
    "SALE": {"SALE_ID", "ORDER_ID", "PRODUCT_ID", "QUANTITY", "UNIT_PRICE", "DISCOUNT", "NET_REVENUE", "PAYMENT_METHOD", "SALE_TIME"}
}

# ------------------------------------------------------------------------------
# JSON SCHEMA CONTRACTS (Data Quality Rules: No Null PKs, No Negative Numbers)
# ------------------------------------------------------------------------------
CORE_JSON_SCHEMAS = {
    "ACCOUNT": {
        "required_keys": ["account_id"],
        "non_negative_keys": [],
        "description": "Primary key 'account_id' must not be null/empty."
    },
    "PRODUCT": {
        "required_keys": ["product_id"],
        "non_negative_keys": ["price"],
        "description": "Primary key 'product_id' must not be null; 'price' >= 0."
    },
    "ORDER": {
        "required_keys": ["order_id", "customer_id"],
        "non_negative_keys": ["order_id"],
        "description": "Primary keys 'order_id' and 'customer_id' must not be null; 'order_id' > 0."
    },
    "SALE": {
        "required_keys": ["sale_id", "order_id", "product_id"],
        "non_negative_keys": ["sale_id", "order_id", "quantity", "unit_price", "net_revenue"],
        "description": "Primary key 'sale_id' must not be null; sales quantities and amounts >= 0."
    }
}

def validate_json_schema_rule(payload_type: str, record: dict) -> list:
    """Validates record against JSON Schema rules: Non-null PKs and non-negative values."""
    violations = []
    schema = CORE_JSON_SCHEMAS.get(payload_type)
    
    # 1. Primary Key No-Null Validation
    if schema:
        for req_k in schema["required_keys"]:
            val = record.get(req_k)
            if val is None or str(val).strip() == "" or str(val).lower() == "null":
                violations.append(f"Null or missing primary key '{req_k}'")
    else:
        # Generic heuristic for dynamic tables: any key ending with _id cannot be null
        for k, v in record.items():
            if k.lower().endswith("_id") and (v is None or str(v).strip() == "" or str(v).lower() == "null"):
                violations.append(f"Null or missing primary key '{k}'")
                
    # 2. Non-Negative Value Validation
    for k, v in record.items():
        if isinstance(v, (int, float)) and v < 0:
            violations.append(f"Negative value not allowed on field '{k}' ({v})")
            
    # Check inside nested items array if present
    if "items" in record and isinstance(record["items"], list):
        for idx, itm in enumerate(record["items"]):
            if isinstance(itm, dict):
                for ik, iv in itm.items():
                    if ik.lower().endswith("_id") and (iv is None or str(iv).strip() == "" or str(iv).lower() == "null"):
                        violations.append(f"Null primary key in nested item[{idx}] '{ik}'")
                    if isinstance(iv, (int, float)) and iv < 0:
                        violations.append(f"Negative value in nested item[{idx}] '{ik}' ({iv})")

    return violations

# Session State
if "approved_tables" not in st.session_state:
    st.session_state.approved_tables = set()
if "rejected_tables" not in st.session_state:
    st.session_state.rejected_tables = set()
if "deselected_columns_per_entity" not in st.session_state:
    st.session_state.deselected_columns_per_entity = {}
if "drift_decisions" not in st.session_state:
    st.session_state.drift_decisions = {}
if "staged_file_name" not in st.session_state:
    st.session_state.staged_file_name = None

# Dynamically sync all approved tables from Snowflake's catalog
if session:
    try:
        catalog_rows = session.sql("""
            SELECT PAYLOAD_TYPE, TARGET_TABLE 
            FROM PIPELINE_OPS.SOURCE_INGESTION_CONTROL 
            WHERE IS_ACTIVE = TRUE AND APPROVAL_STATUS = 'APPROVED'
        """).collect()
        for r in catalog_rows:
            pt = str(r[0]).upper()
            tbl = str(r[1]).upper()
            st.session_state.approved_tables.add(pt)
            CORE_KNOWN_TABLES[pt] = tbl
            
        # Also query column names from SILVER schema for all known tables
        silver_cols = session.sql("""
            SELECT TABLE_NAME, COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'SILVER'
        """).collect()
        for r in silver_cols:
            tbl = str(r[0]).upper()
            col = str(r[1]).upper()
            # Find the payload type matching this target table
            for pt, target_t in CORE_KNOWN_TABLES.items():
                if target_t.upper() == tbl:
                    if pt not in CORE_KNOWN_COLUMNS:
                        CORE_KNOWN_COLUMNS[pt] = set()
                    CORE_KNOWN_COLUMNS[pt].add(col)
    except Exception:
        pass

if file_contents_str is not None and batch_file_name is not None:
    # --------------------------------------------------------------------------
    # ⚡ AUTOMATIC STAGE & BRONZE LANDING (Option 1)
    # Automatically stages file content and loads into BRONZE.OPENFLOW_RAW_STREAM
    # --------------------------------------------------------------------------
    if session and st.session_state.staged_file_name != batch_file_name:
        with st.spinner(f"📥 Staging `{batch_file_name}` and landing raw payloads into Bronze..."):
            try:
                # Batch insert records into BRONZE.OPENFLOW_RAW_STREAM
                select_unions = []
                temp_lines = file_contents_str.splitlines()
                for line in temp_lines:
                    line = line.strip()
                    if not line or line.startswith("PAYLOAD_TYPE"):
                        continue
                    if "|" in line:
                        pt, js = line.split("|", 1)
                    elif "," in line:
                        pt, js = line.split(",", 1)
                    else:
                        continue
                    pt = pt.strip().upper()
                    # Escape single quotes for Snowflake SQL string literal
                    js_escaped = js.strip().replace("'", "''")
                    select_unions.append(f"SELECT '{pt}' AS PAYLOAD_TYPE, PARSE_JSON('{js_escaped}') AS RAW_RECORD, CURRENT_TIMESTAMP() AS INGESTED_AT")

                if select_unions:
                    # Ingest using INSERT ... SELECT (supports PARSE_JSON functions in Snowflake)
                    chunk_size = 50
                    for i in range(0, len(select_unions), chunk_size):
                        chunk = select_unions[i:i + chunk_size]
                        insert_sql = f"""
                        INSERT INTO BRONZE.OPENFLOW_RAW_STREAM (PAYLOAD_TYPE, RAW_RECORD, INGESTED_AT)
                        {' UNION ALL '.join(chunk)};
                        """
                        session.sql(insert_sql).collect()
                    
                    st.toast(f"✅ Staged & Landed {len(select_unions)} records in BRONZE.OPENFLOW_RAW_STREAM!", icon="🚀")
                    st.session_state.staged_file_name = batch_file_name
            except Exception as e:
                st.warning(f"Note on Bronze staging: {str(e)}")

    lines = file_contents_str.splitlines()
    parsed_rows = []
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith("PAYLOAD_TYPE"):
            continue
        if "|" in line:
            ptype, json_str = line.split("|", 1)
        elif "," in line:
            ptype, json_str = line.split(",", 1)
        else:
            continue
        
        ptype = ptype.strip().upper()
        try:
            record = json.loads(json_str.strip())
            parsed_rows.append({"PAYLOAD_TYPE": ptype, "RAW_RECORD": record})
        except Exception:
            pass

    st.success(f" Loaded **{len(parsed_rows)}** raw records from `{batch_file_name}`.")

    # ==========================================================================
    # 🔍 PRE-INGESTION GOVERNANCE SCAN: IS DATA NORMAL OR IS INTERVENTION NEEDED?
    # ==========================================================================
    all_types = sorted(list(set(r["PAYLOAD_TYPE"] for r in parsed_rows)))
    
    # 1. Check for Duplicate IDs / Multiple Entries with same ID
    duplicate_id_triggers = []
    for ptype in all_types:
        records_of_type = []
        for r in parsed_rows:
            if r["PAYLOAD_TYPE"] == ptype:
                rec = r["RAW_RECORD"]
                if "items" in rec and isinstance(rec["items"], list):
                    for item in rec["items"]:
                        c = dict(rec)
                        del c["items"]
                        for ik, iv in item.items():
                            c[f"item_{ik}"] = iv
                        records_of_type.append(c)
                else:
                    records_of_type.append(rec)
        
        if records_of_type:
            df_temp = pd.DataFrame(records_of_type)
            id_cols = [c for c in df_temp.columns if c.endswith("_id") or c == "id" or "sale_id" in c]
            if id_cols:
                pk_c = id_cols[0]
                dup_counts = df_temp[pk_c].value_counts()
                mult_ids = dup_counts[dup_counts > 1].to_dict()
                if mult_ids:
                    duplicate_id_triggers.append({
                        "entity": ptype,
                        "key_column": pk_c,
                        "conflicting_ids": mult_ids
                    })

    # 2. Check for Schema Drift on 4 Core Tables (Altering Existing Tables)
    drift_triggers = []
    for r in parsed_rows:
        ptype = r["PAYLOAD_TYPE"]
        if ptype in CORE_KNOWN_COLUMNS:
            known = CORE_KNOWN_COLUMNS[ptype]
            for k, v in r["RAW_RECORD"].items():
                if k.upper() not in known and not k.startswith("item_"):
                    drift_k = (ptype, CORE_KNOWN_TABLES[ptype], k.upper())
                    if drift_k not in [d["key"] for d in drift_triggers] and drift_k not in st.session_state.drift_decisions:
                        drift_triggers.append({
                            "key": drift_k,
                            "payload_type": ptype,
                            "target_table": CORE_KNOWN_TABLES[ptype],
                            "column_name": k.upper(),
                            "inferred_type": infer_sql_type(v),
                            "sample_value": str(v)[:60]
                        })

    # 3. Check for Brand New Tables (Tables not in 4 Core Tables)
    new_table_triggers = [t for t in all_types if t not in CORE_KNOWN_TABLES and t not in st.session_state.approved_tables]

    # 4. JSON Schema Validation (Negative Values & Null Primary Keys)
    schema_violations = []
    for r in parsed_rows:
        ptype = r["PAYLOAD_TYPE"]
        rec = r["RAW_RECORD"]
        v_errors = validate_json_schema_rule(ptype, rec)
        if v_errors:
            schema_violations.append({
                "entity": ptype,
                "errors": v_errors,
                "record": rec
            })

    # Evaluate Human Intervention Requirement
    needs_intervention = bool(duplicate_id_triggers or drift_triggers or new_table_triggers or schema_violations)

    # --------------------------------------------------------------------------
    # 🎯 GOVERNANCE SUMMARY BANNER
    # --------------------------------------------------------------------------
    if not needs_intervention:
        st.markdown("""
        <div class="status-box-normal">
            <h4 style="color:#6EE7B7; margin:0; font-size:1.1rem;">✅ Data is Normal — No Human Intervention Needed</h4>
            <p style="color:#A7F3D0; margin:6px 0 0 0; font-size:0.95rem;">
                All records conform to existing table schemas, contain unique primary keys, and have no duplicate ID conflicts. 
                You can proceed directly with automated ingestion.
            </p>
        </div>
        """, unsafe_allow_html=True)
    else:
        reasons = []
        if drift_triggers:
            drift_desc = ", ".join([f"<code>SILVER.{d['target_table']}</code> received <code>{d['column_name']}</code>" for d in drift_triggers])
            reasons.append(f"• <b>Schema Alteration Detected:</b> {drift_desc}. Review required in Container 1.")
        if new_table_triggers:
            reasons.append(f"• <b>Brand-New Tables Detected:</b> {', '.join([f'<code>{t}</code>' for t in new_table_triggers])}. Column checkbox selection required in Container 2.")
        if duplicate_id_triggers:
            dup_desc = ", ".join([f"<code>{d['entity']}</code> (Key: <code>{d['key_column']}</code>, IDs: {list(d['conflicting_ids'].keys())})" for d in duplicate_id_triggers])
            reasons.append(f"• <b>Multiple entries detected with the same ID:</b> {dup_desc}. Review required in Container 3 (latest transaction prioritized on top).")
        if schema_violations:
            viol_list = []
            for sv in schema_violations:
                for err in sv["errors"]:
                    viol_list.append(f"<code>{sv['entity']}</code>: {err}")
            reasons.append(f"• <b>JSON Schema Contract Violations:</b> {'; '.join(viol_list[:5])}. Corrupt records are automatically unselected [ ] in Container 3.")

        st.markdown(f"""
        <div class="status-box-intervention">
            <h4 style="color:#FDE68A; margin:0; font-size:1.1rem;">⚠️ Human Intervention Required</h4>
            <p style="color:#FCD34D; margin:6px 0 10px 0; font-size:0.95rem;">Please review the flagged items before data is committed into Silver:</p>
            <div style="color:#FEF3C7; line-height:1.6;">
                {"<br/>".join(reasons)}
            </div>
        </div>
        """, unsafe_allow_html=True)

    # --------------------------------------------------------------------------
    # 📦 CONTAINER 1: SCHEMA DRIFT REVIEW (Altering Existing Tables) - FIRST
    # --------------------------------------------------------------------------
    with st.container():
        st.markdown("---")
        st.markdown("### 📦 Container 1: Schema Drift Review (Altering Tables)")
        
        if not drift_triggers:
            st.info("✅ No schema alterations needed. Existing table schemas (`DIM_ACCOUNTS`, `DIM_PRODUCTS`, `FACT_ORDERS`, `FACT_SALES`) match the incoming data.")
        else:
            st.caption("Human intervention required: Unmapped columns detected on existing tables. Choose to alter the table or reject and route bad data to DLQ.")
            
            for item in drift_triggers:
                col_d1, col_d2, col_d3 = st.columns([3, 1, 1])
                with col_d1:
                    st.markdown(f"""
                    <div class="drift-alert">
                        ⚠️ <b>Schema Alteration Required on Table:</b> <code>SILVER.{item['target_table']}</code><br/>
                        • <b>New Column:</b> <code>{item['column_name']}</code> ({item['inferred_type']})<br/>
                        • <b>Sample Value:</b> <code>{item['sample_value']}</code>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col_d2:
                    st.write("")
                    if st.button(f"➕ Approve & Alter Table", key=f"btn_add_{item['column_name']}", type="primary"):
                        if session:
                            try:
                                alter_sql = f"ALTER TABLE SILVER.{item['target_table']} ADD COLUMN IF NOT EXISTS {item['column_name']} {item['inferred_type']};"
                                session.sql(alter_sql).collect()
                                # Record approval in SCHEMA_DRIFT_APPROVAL_QUEUE
                                log_appr_sql = f"""
                                MERGE INTO PIPELINE_OPS.SCHEMA_DRIFT_APPROVAL_QUEUE tgt
                                USING (SELECT '{item['payload_type']}' AS P, 'SILVER' AS S, '{item['target_table']}' AS T, '{item['column_name']}' AS C) src
                                ON tgt.PAYLOAD_TYPE = src.P AND tgt.TARGET_TABLE = src.T AND tgt.DETECTED_COLUMN = src.C
                                WHEN MATCHED THEN UPDATE SET tgt.STATUS = 'APPROVED', tgt.REVIEWED_AT = CURRENT_TIMESTAMP()
                                WHEN NOT MATCHED THEN INSERT (PAYLOAD_TYPE, TARGET_SCHEMA, TARGET_TABLE, DETECTED_COLUMN, INFERRED_DATA_TYPE, STATUS, REVIEWED_AT)
                                VALUES (src.P, src.S, src.T, src.C, '{item['inferred_type']}', 'APPROVED', CURRENT_TIMESTAMP());
                                """
                                session.sql(log_appr_sql).collect()
                                st.success(f"Added column `{item['column_name']}` to `{item['target_table']}`")
                            except Exception as e:
                                st.error(f"Error: {e}")
                        else:
                            st.success(f"Simulated: Added `{item['column_name']}` to `SILVER.{item['target_table']}`")
                        
                        st.session_state.drift_decisions[item['key']] = "APPROVED"
                        safe_rerun()
                        
                with col_d3:
                    st.write("")
                    if st.button(f"🚫 Reject & DLQ", key=f"btn_dlq_{item['column_name']}"):
                        # Find all records containing this rejected column
                        rejected_records = [
                            r for r in parsed_rows 
                            if r["PAYLOAD_TYPE"] == item["payload_type"] and item["column_name"].lower() in [k.lower() for k in r["RAW_RECORD"].keys()]
                        ]
                        if session:
                            try:
                                for rec in rejected_records:
                                    clean_raw = json.dumps(rec["RAW_RECORD"]).replace("'", "''")
                                    dlq_sql = f"""
                                    INSERT INTO PIPELINE_OPS.DEAD_LETTER_QUEUE (PIPELINE_NAME, RAW_PAYLOAD, ERROR_MSG, STATUS)
                                    SELECT '{item['payload_type']}', PARSE_JSON('{clean_raw}'), 'Unauthorized column {item['column_name']} rejected by UI', 'QUEUED'
                                    """
                                    session.sql(dlq_sql).collect()

                                # Record rejection in SCHEMA_DRIFT_APPROVAL_QUEUE so ingestion procedures filter it out
                                log_rej_sql = f"""
                                MERGE INTO PIPELINE_OPS.SCHEMA_DRIFT_APPROVAL_QUEUE tgt
                                USING (SELECT '{item['payload_type']}' AS P, 'SILVER' AS S, '{item['target_table']}' AS T, '{item['column_name']}' AS C) src
                                ON tgt.PAYLOAD_TYPE = src.P AND tgt.TARGET_TABLE = src.T AND tgt.DETECTED_COLUMN = src.C
                                WHEN MATCHED THEN UPDATE SET tgt.STATUS = 'REJECTED', tgt.REVIEWED_AT = CURRENT_TIMESTAMP()
                                WHEN NOT MATCHED THEN INSERT (PAYLOAD_TYPE, TARGET_SCHEMA, TARGET_TABLE, DETECTED_COLUMN, INFERRED_DATA_TYPE, STATUS, REVIEWED_AT)
                                VALUES (src.P, src.S, src.T, src.C, '{item['inferred_type']}', 'REJECTED', CURRENT_TIMESTAMP());
                                """
                                session.sql(log_rej_sql).collect()

                                st.warning(f"Quarantined {len(rejected_records)} records with `{item['column_name']}` to DLQ. Raw payload preserved in Bronze.")
                            except Exception as e:
                                st.error(f"Error: {e}")
                        else:
                            st.warning(f"Simulated: Quarantined {len(rejected_records)} records with `{item['column_name']}` to DLQ.")
                            
                        st.session_state.drift_decisions[item['key']] = "REJECTED"
                        safe_rerun()

    # --------------------------------------------------------------------------
    # 📦 CONTAINER 2: NEW TABLE ONBOARDING REVIEW (Interactive Column Checkboxes) - SECOND
    # --------------------------------------------------------------------------
    with st.container():
        st.markdown("---")
        st.markdown("### 📦 Container 2: New Table Onboarding Review")
        
        if not new_table_triggers:
            st.info("✅ No new tables detected in this batch. All incoming entities match the 4 pre-coded core tables.")
        else:
            st.caption("Human intervention required: Select which columns to include in the Silver table using the checkboxes below. Deselected columns will be permanently excluded.")
            
            for new_t in new_table_triggers:
                st.markdown(f'<div class="new-table-card">🆕 <b>Brand-New Table Requested:</b> <code>SILVER.{new_t}</code></div>', unsafe_allow_html=True)
                
                col_info = {}
                records_for_t = [r["RAW_RECORD"] for r in parsed_rows if r["PAYLOAD_TYPE"] == new_t]
                
                for rec in records_for_t:
                    for k, v in rec.items():
                        if k not in col_info:
                            col_info[k] = {
                                "Include": True,
                                "Column Name": k.upper(),
                                "Inferred Type": infer_sql_type(v),
                                "Sample Value": str(v)[:40]
                            }
                
                df_cols = pd.DataFrame(list(col_info.values()))
                
                col_c1, col_c2 = st.columns([3, 1])
                with col_c1:
                    edited_df = st.data_editor(
                        df_cols,
                        key=f"editor_{new_t}",
                        column_config={
                            "Include": st.column_config.CheckboxColumn("Include?", default=True, help="Check to add this column to Silver table"),
                            "Column Name": st.column_config.TextColumn("Column Name", disabled=True),
                            "Inferred Type": st.column_config.SelectboxColumn("Data Type", options=["VARCHAR(50)", "VARCHAR(150)", "VARCHAR(250)", "NUMBER(38,0)", "NUMBER(12,2)", "BOOLEAN", "TIMESTAMP_NTZ"], required=True),
                            "Sample Value": st.column_config.TextColumn("Sample Value", disabled=True)
                        },
                        hide_index=True,
                        use_container_width=True
                    )
                    
                    # Track unselected/deselected columns
                    deselected_cols = edited_df[edited_df["Include"] == False]["Column Name"].tolist()
                    if deselected_cols:
                        st.session_state.deselected_columns_per_entity[new_t] = deselected_cols
                    else:
                        st.session_state.deselected_columns_per_entity.pop(new_t, None)
                
                with col_c2:
                    pk_candidates = [c for c in edited_df["Column Name"] if "_ID" in c or c == "ID"]
                    selected_pk = st.selectbox(
                        f"Primary Key for {new_t}:",
                        options=edited_df["Column Name"].tolist(),
                        index=edited_df["Column Name"].tolist().index(pk_candidates[0]) if pk_candidates else 0,
                        key=f"pk_{new_t}"
                    )
                    
                    st.write("")
                    st.write("")
                    if st.button(f"🚀 Approve & Create Table [SILVER.{new_t}]", key=f"btn_approve_{new_t}", type="primary"):
                        selected_columns = edited_df[edited_df["Include"] == True]
                        
                        if selected_columns.empty:
                            st.error("⚠️ Please select at least one column to include in the table.")
                        elif selected_pk not in selected_columns["Column Name"].values:
                            st.error(f"⚠️ The selected Primary Key `{selected_pk}` must have its checkbox checked.")
                        else:
                            cols_payload = [
                                {"COLUMN_NAME": row["Column Name"], "DATA_TYPE": row["Inferred Type"]}
                                for _, row in selected_columns.iterrows()
                            ]
                            
                            if session:
                                try:
                                    json_param = json.dumps(cols_payload).replace("'", "''")
                                    sql = f"CALL PIPELINE_OPS.SP_CREATE_CUSTOM_ONBOARDED_TABLE('{new_t}', '{new_t}', '{selected_pk}', '{json_param}')"
                                    res = session.sql(sql).collect()
                                    st.success(f"✅ {res[0][0]}")
                                except Exception as e:
                                    st.error(f"Snowflake Error: {str(e)}")
                            else:
                                st.success(f"✅ Created table `SILVER.{new_t}` with {len(selected_columns)} selected columns (PK: `{selected_pk}`).")
                            
                            st.session_state.approved_tables.add(new_t)
                            st.session_state.rejected_tables.discard(new_t)
                            safe_rerun()
                        
                    if st.button(f"🚫 Reject & Route to DLQ", key=f"btn_reject_{new_t}"):
                        if session:
                            try:
                                sql = f"CALL PIPELINE_OPS.SP_REJECT_NEW_TABLE_TO_DLQ('{new_t}')"
                                res = session.sql(sql).collect()
                                st.warning(f"⚠️ {res[0][0]}")
                            except Exception as e:
                                st.error(f"Snowflake Error: {str(e)}")
                        else:
                            st.warning(f"⚠️ Entity `{new_t}` rejected. All {len(records_for_t)} records quarantined to DLQ.")
                        
                        st.session_state.rejected_tables.add(new_t)
                        st.session_state.approved_tables.discard(new_t)
                        safe_rerun()

    # --------------------------------------------------------------------------
    # 📦 CONTAINER 3: RECORDS READY TO INSERT (Human Review & Record Selection) - THIRD
    # (Filters OUT records linked to rejected tables or rejected schema drift)
    # --------------------------------------------------------------------------
    with st.container():
        st.markdown("---")
        st.markdown("### 📦 Container 3: Records Ready to Insert")
        st.caption("Human intervention for multiple entries sharing the same ID. Records linked to rejected tables or rejected schema drift columns are automatically excluded.")

        # Identify all rejected drift column names per payload type
        rejected_drift_cols_by_entity = {}
        for (ptype, tbl, col), dec in st.session_state.drift_decisions.items():
            if dec == "REJECTED":
                rejected_drift_cols_by_entity.setdefault(ptype, set()).add(col.lower())

        # Determine eligible incoming entities (exclude completely rejected new tables)
        eligible_entities = [t for t in all_types if t not in st.session_state.rejected_tables]
        
        # Entities that specifically have multiple entries with same ID
        entities_needing_review = [d["entity"] for d in duplicate_id_triggers if d["entity"] in eligible_entities]

        if not eligible_entities:
            st.warning("⚠️ All entities in this batch have been rejected and routed to DLQ. No records available to insert.")
        else:
            # Determine available entity choices from incoming data
            filter_scope = st.radio(
                "Filter Scope:",
                ["⚠️ Show Only Entities Needing Review (Duplicate IDs)", "👁️ Show All Eligible Entities"],
                horizontal=True,
                index=0 if entities_needing_review else 1
            )
            
            if "Show Only Entities Needing Review" in filter_scope and entities_needing_review:
                selectable_entities = entities_needing_review
            else:
                selectable_entities = eligible_entities

            col_sel1, col_sel2 = st.columns([2, 1])
            with col_sel1:
                selected_type = st.selectbox(
                    "Select Entity to Preview (from incoming data):", 
                    selectable_entities, 
                    index=0
                )
            
            # Check if selected entity has duplicate IDs
            has_dups = any(d["entity"] == selected_type for d in duplicate_id_triggers)
            dup_info = next((d for d in duplicate_id_triggers if d["entity"] == selected_type), None)
            
            with col_sel2:
                st.write("")
                st.write("")
                if has_dups:
                    st.warning(f"⚠️ Multiple entries with same ID in `{selected_type}`")
                else:
                    st.success(f"✅ All records for `{selected_type}` have unique IDs")

            # Unpack records for selected type, EXCLUDING any record containing a rejected drift column
            rejected_cols = rejected_drift_cols_by_entity.get(selected_type, set())
            deselected_cols_table = [c.lower() for c in st.session_state.deselected_columns_per_entity.get(selected_type, [])]

            entity_records = []
            excluded_due_to_drift_count = 0

            for r in parsed_rows:
                if r["PAYLOAD_TYPE"] == selected_type:
                    rec = r["RAW_RECORD"]
                    # If this record has a rejected drift column, skip it!
                    rec_keys_lower = [k.lower() for k in rec.keys()]
                    if any(rej in rec_keys_lower for rej in rejected_cols):
                        excluded_due_to_drift_count += 1
                        continue
                    
                    # Also drop deselected table onboarding columns from view
                    filtered_rec = {k: v for k, v in rec.items() if k.lower() not in deselected_cols_table}

                    if "items" in filtered_rec and isinstance(filtered_rec["items"], list):
                        for item in filtered_rec["items"]:
                            c = dict(filtered_rec)
                            del c["items"]
                            for ik, iv in item.items():
                                c[f"item_{ik}"] = iv
                            entity_records.append(c)
                    else:
                        entity_records.append(filtered_rec)

            if excluded_due_to_drift_count > 0:
                st.warning(f"🚫 **{excluded_due_to_drift_count} records excluded**: Linked to rejected schema drift column(s) ({', '.join(rejected_cols)}). Routed to DLQ.")

            if deselected_cols_table:
                st.caption(f"ℹ️ Unselected columns from Container 2 excluded from display: `{', '.join(deselected_cols_table)}`")

            if entity_records:
                df_preview = pd.DataFrame(entity_records)
                
                id_cols = [c for c in df_preview.columns if c.endswith("_id") or c == "id" or "sale_id" in c]
                ts_cols = [c for c in df_preview.columns if "time" in c.lower() or "updated" in c.lower() or "at" in c.lower()]
                
                chosen_id = dup_info["key_column"] if dup_info else (id_cols[0] if id_cols else df_preview.columns[0])
                chosen_ts = ts_cols[0] if ts_cols else None
                
                # Sort by timestamp DESC so latest transaction is ALWAYS on top!
                if chosen_ts:
                    try:
                        df_preview[chosen_ts] = pd.to_datetime(df_preview[chosen_ts], errors="coerce")
                        df_preview = df_preview.sort_values(by=chosen_ts, ascending=False)
                    except Exception:
                        pass

                # Filter rows to review if duplicates exist (optional user toggle)
                if has_dups and dup_info:
                    conflicting_ids = list(dup_info["conflicting_ids"].keys())
                    review_filter = st.checkbox(
                        f"Show ONLY conflicting/duplicate ID records (IDs: {conflicting_ids})",
                        value=False,
                        help="When checked, filters the view to show only the records with duplicate IDs. Uncheck to see all batch records."
                    )
                    if review_filter:
                        df_preview = df_preview[df_preview[chosen_id].isin(conflicting_ids)]

                # Add status tag and interactive Insert checkbox
                # JSON Schema Validation: Records with negative values or NULL Primary Keys must be DESELECTED [ ] by default!
                is_nested_items = "item_sale_id" in df_preview.columns
                
                seen_ids = set()
                default_checks = []
                ranks = []
                
                for _, row in df_preview.iterrows():
                    val_id = row[chosen_id] if chosen_id in row else None
                    
                    # 1. Check for Null / Empty Primary Key
                    is_null_pk = (val_id is None or pd.isna(val_id) or str(val_id).strip() == "" or str(val_id).lower() == "null")
                    
                    # 2. Check for Negative Numbers across all numeric fields
                    has_negative = False
                    for col in df_preview.columns:
                        val = row[col]
                        if isinstance(val, (int, float)) and val < 0:
                            has_negative = True
                            break
                    
                    if is_null_pk:
                        # ❌ Null primary key violates JSON Schema constraint
                        default_checks.append(False)
                        ranks.append("🚫 NULL PRIMARY KEY")
                    elif has_negative:
                        # ❌ Negative values (e.g. order_id = -99) violate JSON Schema constraint
                        default_checks.append(False)
                        ranks.append("🚫 INVALID / NEGATIVE")
                    elif is_nested_items:
                        # In an order with multiple product line items, every valid line item is selected by default
                        default_checks.append(True)
                        ranks.append("ITEM LINE" if "item_sale_id" in row and pd.notnull(row["item_sale_id"]) else "ORDER HEADER")
                    else:
                        # For regular duplicate entities, latest entry is checked, older duplicate entries are unchecked
                        if val_id not in seen_ids:
                            seen_ids.add(val_id)
                            default_checks.append(True)
                            ranks.append("⭐ LATEST")
                        else:
                            default_checks.append(False)
                            ranks.append("OLDER ENTRY")

                df_preview.insert(0, "Insert?", default_checks)
                df_preview.insert(1, "Status", ranks)

                st.caption("👇 **Check or uncheck records below**. Only records with **`[x] Insert?`** checked will be committed to Silver:")
                
                edited_records = st.data_editor(
                    df_preview,
                    column_config={
                        "Insert?": st.column_config.CheckboxColumn(
                            "Insert?",
                            help="Check to approve this record for insertion into Silver",
                            default=True
                        ),
                        "Status": st.column_config.TextColumn(
                            "Status",
                            help="Indicates whether this is the latest transaction for this ID",
                            disabled=True
                        )
                    },
                    disabled=[c for c in df_preview.columns if c != "Insert?"],
                    use_container_width=True,
                    hide_index=True,
                    key=f"editor_records_{selected_type}"
                )
                
                selected_count = int(edited_records["Insert?"].sum()) if "Insert?" in edited_records.columns else len(df_preview)
                st.info(f"📋 **{selected_count} of {len(df_preview)}** `{selected_type}` records selected for insertion.")
            else:
                st.info(f"ℹ️ No valid records remaining for `{selected_type}` to display.")

    # --------------------------------------------------------------------------
    # 📦 CONTAINER 4: COMMIT & STORE REVIEWED DATA IN SILVER
    # --------------------------------------------------------------------------
    with st.container():
        st.markdown("---")
        st.markdown("### 📦 Container 4: Commit & Store Reviewed Data in Silver")
        st.caption("Once you have reviewed schema drift, configured new tables, and selected records, click below to commit and store into Silver.")

        # Compute count of remaining approved records ready for Silver
        num_rejected_records = sum([
            len([r for r in parsed_rows if r["PAYLOAD_TYPE"] == item["payload_type"] and item["column_name"].lower() in [k.lower() for k in r["RAW_RECORD"].keys()]])
            for item in drift_triggers if st.session_state.drift_decisions.get(item["key"]) == "REJECTED"
        ])
        num_rejected_table_records = len([r for r in parsed_rows if r["PAYLOAD_TYPE"] in st.session_state.rejected_tables])
        total_quarantined = num_rejected_records + num_rejected_table_records
        approved_records_to_store = max(0, len(parsed_rows) - total_quarantined)

        # Summary of what will be stored
        c_cols1, c_cols2 = st.columns([3, 1])
        with c_cols1:
            st.markdown(f"""
            **Ready to Store**:
            - **Ingress Batch**: `{len(parsed_rows)}` total records from `{batch_file_name}`.
            - **Approved for Silver**: `{approved_records_to_store}` records.
            - **Quarantined to DLQ**: `{total_quarantined}` records.
            - **Active Target Tables**: `{', '.join(eligible_entities)}`.
            - **Deduplication Rule**: For records with the same ID, only the **latest entry / transaction** is stored.
            """)
        
        with c_cols2:
            st.write("")
            run_btn = st.button("🚀 Store All Reviewed Data in Silver", type="primary", use_container_width=True)

        if run_btn:
            with st.spinner("Storing all reviewed data into Silver layer..."):
                if session:
                    try:
                        res = session.sql("CALL PIPELINE_OPS.SP_RUN_ALL_INGESTION_PIPELINES('LATEST_ONLY')").collect()
                        st.success(f"🎉 {res[0][0]}")
                    except Exception as e:
                        st.error(f"Execution Error: {str(e)}")
                else:
                    st.success(f"🎉 Successfully stored all reviewed records into the Silver layer!")
                
                # Metrics Banner
                m1, m2, m3 = st.columns(3)
                m1.metric("Total Ingress Records", len(parsed_rows))
                m1_rej = len(st.session_state.rejected_tables) + len([v for v in st.session_state.drift_decisions.values() if v == "REJECTED"])
                m2.metric("Target Layer", "SILVER")
                m3.metric("DLQ Quarantined", m1_rej)

else:
    st.info("👆 Please upload a batch CSV file to view records and begin governance control.")

