import pandas as pd
import great_expectations as gx
from sqlalchemy import create_engine
import sys
import logging

# --- Database Configuration ---
# Connecting to the Data Warehouse via Docker network
# DB_URL uses the service name 'postgres' defined in docker-compose
DB_URL = "postgresql://caelan:password123@postgres:5432/nyc_warehouse"
engine = create_engine(DB_URL)

def run_quality_check():
    """
    Executes a Hybrid Data Quality Validation:
    1. Primary: Great Expectations (GX) for standardized validation.
    2. Fallback: Manual Pandas-based validation for critical redundancy.
    """
    print("🚀 Initiating Data Quality (DQ) Validation...")
    
    # 1. Data Retrieval from Warehouse
    try:
        df = pd.read_sql("SELECT * FROM monthly_taxi_summary", engine)
        print(f"📊 Dataset successfully ingested for validation. Row count: {len(df)}")
        
        if df.empty:
            print("⚠️ Warning: Target table is empty! Aborting validation.")
            return

    except Exception as e:
        print(f"❌ Database Connection Error: {e}")
        sys.exit(1)

    # 2. Primary Validation: Great Expectations (Standard Industry Framework)
    try:
        print("🔧 Running validation via Great Expectations...")
        context = gx.get_context()
        
        # GX 1.x Pandas Data Source
        validator = context.data_sources.pandas_default.read_dataframe(df)
        
        # --- Validation Rules (Expectations) ---
        res1 = validator.expect_column_values_to_not_be_null("VendorID")
        res2 = validator.expect_column_values_to_be_between("total_trips", min_value=1)
        res3 = validator.expect_column_values_to_be_between("avg_fare", min_value=0.1)
        
        all_passed = res1.success and res2.success and res3.success
        
        if all_passed:
            print("✅ [GX] Data Quality Audit PASSED.")
        else:
            print("❌ [GX] Data Quality Audit FAILED: Integrity constraints violated.")
            
    except Exception as e:
        # 3. Fallback Mechanism: Manual Data Integrity Checks
        print(f"⚠️ Great Expectations runtime error: {e}")
        print("🔄 Switching to Manual Validation (Fallback Mode)...")
        
        integrity_issues = []
        
        # Rule 1: Null Check
        if df['VendorID'].isnull().any():
            integrity_issues.append("Null values detected in 'VendorID'.")
            
        # Rule 2: Logical Range Check (Trips)
        if (df['total_trips'] <= 0).any():
            integrity_issues.append("Invalid 'total_trips' (values must be > 0).")
            
        # Rule 3: Logical Range Check (Fare)
        if (df['avg_fare'] <= 0).any():
            integrity_issues.append("Anomalous 'avg_fare' (values must be > 0).")
            
        if not integrity_issues:
            print("✅ [Manual] Fallback validation PASSED.")
        else:
            print("❌ [Manual] Data Integrity Check FAILED:")
            for issue in integrity_issues:
                print(f"   - {issue}")

if __name__ == "__main__":
    run_quality_check()