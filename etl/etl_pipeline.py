import pandas as pd
import sqlite3
import json
from datetime import datetime

DB_PATH = "db/lineage.db"

def log_lineage(dataset_id, source, operation, metadata):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO lineage_log (dataset_id, source, operation, timestamp, metadata)
        VALUES (?, ?, ?, ?, ?)
    """, (
        dataset_id,
        source,
        operation,
        datetime.now().isoformat(),
        json.dumps(metadata)
    ))

    conn.commit()
    conn.close()


# ---------------- ETL START ----------------

dataset_id = "sales_001_v1"

# Step 1: Read raw data
df = pd.read_csv("data/raw_sales.csv")

log_lineage(
    dataset_id,
    "raw_sales.csv",
    "read_csv",
    {
        "rows": len(df),
        "columns": list(df.columns),
        "dtypes": df.dtypes.astype(str).to_dict()
    }
)

# Step 2: Handle missing values
null_rows = df.isnull().sum().to_dict()
df_cleaned = df.dropna()

log_lineage(
    dataset_id,
    "raw_sales.csv",
    "drop_null_values",
    {
        "nulls_before": null_rows,
        "rows_after": len(df_cleaned)
    }
)

# Step 3: Type casting (realistic transformation)
df_cleaned["amount"] = df_cleaned["amount"].astype(int)

log_lineage(
    dataset_id,
    "raw_sales.csv",
    "type_cast",
    {
        "column": "amount",
        "from": "float",
        "to": "int"
    }
)

# Step 4: Create processed dataset (versioning)
new_dataset_id = "sales_001_v2"

log_lineage(
    new_dataset_id,
    "sales_001_v1",
    "create_new_version",
    {
        "reason": "data cleaned and type standardized"
    }
)

# ---------------- ETL FOR SALES_002 ----------------

dataset_id = "sales_002_v1"

df2 = pd.read_csv("data/raw_sales.csv")

log_lineage(
    dataset_id,
    "raw_sales.csv",
    "read_csv",
    {
        "rows": len(df2),
        "columns": list(df2.columns),
        "dtypes": df2.dtypes.astype(str).to_dict()
    }
)

null_rows_2 = df2.isnull().sum().to_dict()
df2_cleaned = df2.dropna()

log_lineage(
    dataset_id,
    "raw_sales.csv",
    "drop_null_values",
    {
        "nulls_before": null_rows_2,
        "rows_after": len(df2_cleaned)
    }
)

df2_cleaned["amount"] = df2_cleaned["amount"].astype(int)

log_lineage(
    dataset_id,
    "raw_sales.csv",
    "type_cast",
    {
        "column": "amount",
        "from": "float",
        "to": "int"
    }
)

new_dataset_id = "sales_002_v2"

log_lineage(
    new_dataset_id,
    "sales_002_v1",
    "create_new_version",
    {
        "reason": "cleaned second sales dataset"
    }
)

# ---------------- AGGREGATED DATASET ----------------

dataset_id = "sales_001_agg_v1"

df_agg = df_cleaned.groupby("order_id").sum().reset_index()

log_lineage(
    dataset_id,
    "sales_001_v2",
    "aggregation",
    {
        "group_by": ["order_id"],
        "aggregation": "sum(amount)",
        "rows": len(df_agg)
    }
)

# ---------------- ML TRAIN DATASET ----------------

dataset_id = "sales_001_ml_train_v1"

df_ml = df_cleaned.copy()
df_ml["amount_log"] = df_ml["amount"].apply(lambda x: x)

log_lineage(
    dataset_id,
    "sales_001_v2",
    "feature_engineering",
    {
        "features_added": ["amount_log"],
        "rows": len(df_ml)
    }
)

log_lineage(
    dataset_id,
    "sales_001_v2",
    "prepare_for_ml",
    {
        "target_column": "amount",
        "features": ["amount_log"]
    }
)
