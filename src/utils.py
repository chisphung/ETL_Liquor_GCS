import pandas as pd
from sqlalchemy import text
from datetime import datetime

# Get list of processed files
def get_processed_files(engine):
    try:
        return set(pd.read_sql("SELECT file_name FROM Processed_Files", engine)['file_name'])
    except:
        return set()

# Process SCD Type 2 for dimensions with improved merge technique
def process_scd_type2(df, dim_table, key_col, attributes, conn):
    """Handle Slowly Changing Dimension Type 2 changes"""
    current_dim = pd.read_sql(f"SELECT * FROM {dim_table} WHERE is_active = 1", conn)
    
    # Create a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    if current_dim.empty:
        # Initial load - all records are new
        df = df.assign(
            start_date=datetime.now().date(),
            end_date=None,
            is_active=True
        )
        print(datetime.now().date())
        if not df.empty:
            df.to_sql(dim_table, conn, if_exists='append', index=False)
        return
    
    # Find new and changed records
    merged = df.merge(current_dim, on=key_col, how='outer', 
                     suffixes=('', '_current'), indicator=True)
    
    # New records
    new_records = merged[merged['_merge'] == 'left_only'][[key_col] + attributes].copy()
    if not new_records.empty:
        new_records = new_records.assign(
            start_date=datetime.now().date(),
            end_date=None,
            is_active=True
        )
        if not new_records.empty:
            new_records.to_sql(dim_table, conn, if_exists='append', index=False)
    
    # Changed records
    changed_records = merged[merged['_merge'] == 'both'].copy()
    for attr in attributes:
        changed_records = changed_records[changed_records[attr] != changed_records[f"{attr}_current"]]
    
    if not changed_records.empty:
        # Expire old records
        conn.execute(text(f"""
            UPDATE {dim_table}
            SET end_date = :end_date, is_active = 0
            WHERE {key_col} = :key_val AND is_active = 1
        """), [{'end_date': datetime.now().date(), 'key_val': row[key_col]} 
              for _, row in changed_records.iterrows()])
        
        # Insert new versions
        new_versions = changed_records[[key_col] + attributes].copy()
        new_versions = new_versions.assign(
            start_date=datetime.now().date(),
            end_date=None,
            is_active=True
        )
        if not new_versions.empty:
            new_versions.to_sql(dim_table, conn, if_exists='append', index=False)