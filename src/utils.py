import pandas as pd
from datetime import datetime
import pandas_gbq
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from src.config import PROJECT_ID, DATASET_ID

def get_processed_files(project_id=PROJECT_ID, dataset_name=DATASET_ID):
    """
    Retrieve a set of processed file names from the Processed_Files table in BigQuery.
    
    Args:
        project_id (str): Google Cloud project ID.
        dataset_name (str): BigQuery dataset name.
    
    Returns:
        set: Set of processed file names.
    """
    try:
        print(f"Fetching processed files from {dataset_name}.Processed_Files...")
        query = f"SELECT file_name FROM {dataset_name}.Processed_Files"
        return set(pandas_gbq.read_gbq(query, project_id=PROJECT_ID)['file_name'])
    except GoogleAPIError as e:
        if "404" in str(e):  # Table not found
            print(f"Processed_Files table not found. Creating it...")
            client = bigquery.Client(project=PROJECT_ID)
            schema = [
                bigquery.SchemaField("file_name", "STRING"),
                bigquery.SchemaField("processed_timestamp", "TIMESTAMP")
            ]
            table = bigquery.Table(f"{PROJECT_ID}.{dataset_name}.Processed_Files", schema=schema)
            client.create_table(table)
            return set()
        print(f"BigQuery error fetching processed files: {e}")
        return set()
    except Exception as e:
        print(f"Unexpected error fetching processed files: {e}")
        return set()

def process_scd_type2(df, dim_table, key_col, attributes, project_id=PROJECT_ID, dataset_name=DATASET_ID):
    """
    Handle Slowly Changing Dimension Type 2 changes for a dimension table in BigQuery.
    
    Args:
        df (pd.DataFrame): Input DataFrame with new/updated dimension data.
        dim_table (str): Name of the dimension table (e.g., 'Store_Dim').
        key_col (str): Primary key column name.
        attributes (list): List of attribute columns to check for changes.
        project_id (str): Google Cloud project ID.
        dataset_name (str): BigQuery dataset name.
    """
    client = bigquery.Client(project=PROJECT_ID)
    df = df.copy()
    # for col in df.columns:
    #     if df[col].dtype == 'object':
    #         try:
    #             df[col] = pd.to_numeric(df[col], errors='coerce')
    #         except:
    #             pass

    # Read current active records
    try:
        query = f"SELECT {key_col}, {', '.join(attributes)}, start_date, end_date, is_active " \
                f"FROM {PROJECT_ID}.{dataset_name}.{dim_table} WHERE is_active = TRUE"
        current_dim = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
    except GoogleAPIError as e:
        print(f"Error reading {dim_table}: {e}")
        current_dim = pd.DataFrame()

    if current_dim.empty:
        df = df.assign(start_date=datetime.now().date(), end_date=None, is_active=True)
        if not df.empty:
            try:
                pandas_gbq.to_gbq(df, f"{dataset_name}.{dim_table}", project_id=PROJECT_ID, if_exists='append')
                print(f"Inserted {len(df)} new records into {dim_table}")
            except GoogleAPIError as e:
                print(f"Error inserting new records into {dim_table}: {e}")
        else:
            print(f"No new records for {dim_table}")
        return

    # Merge to find new and changed records
    merged = df.merge(current_dim, on=key_col, how='outer', suffixes=('', '_current'), indicator=True)
    
    # New records
    new_records = merged[merged['_merge'] == 'left_only'][[key_col] + attributes].copy()
    if not new_records.empty:
        new_records = new_records.assign(start_date=datetime.now().date(), end_date=None, is_active=True)
        try:
            pandas_gbq.to_gbq(new_records, f"{dataset_name}.{dim_table}", project_id=PROJECT_ID, if_exists='append')
            print(f"Inserted {len(new_records)} new records into {dim_table}")
        except GoogleAPIError as e:
            print(f"Error inserting new records into {dim_table}: {e}")
    else:
        print(f"No new records for {dim_table}")

    # Changed records
    changed_records = merged[merged['_merge'] == 'both'].copy()
    for attr in attributes:
        changed_records = changed_records[changed_records[attr] != changed_records[f"{attr}_current"]]

    if not changed_records.empty:
        new_versions = changed_records[[key_col] + attributes].copy()
        new_versions = new_versions.assign(start_date=datetime.now().date(), end_date=None, is_active=True)
        
        # Use MERGE to update existing records and insert new versions
        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.{dim_table}` T
        USING (SELECT * FROM UNNEST(@new_data)) S
        ON T.{key_col} = S.{key_col} AND T.is_active = TRUE
        WHEN MATCHED THEN
            UPDATE SET end_date = CURRENT_DATE(), is_active = FALSE
        WHEN NOT MATCHED THEN
            INSERT ({key_col}, {', '.join(attributes)}, start_date, end_date, is_active)
            VALUES ({key_col}, {', '.join(attributes)}, CURRENT_DATE(), NULL, TRUE)
        """
        try:
            # job_config = bigquery.QueryJobConfig(
            #     query_parameters=[
            #         bigquery.ArrayQueryParameter('new_data', 'STRUCT', new_versions.to_dict('records'))
            #     ]
            # )
            # client.query(merge_query, job_config=job_config).result()
            temp_table_id = f"{dataset_name}.temp_new_versions"
            pandas_gbq.to_gbq(new_versions, temp_table_id, project_id=PROJECT_ID, if_exists='replace')
            merge_query = f"""
            MERGE `{project_id}.{dataset_name}.{dim_table}` T
            USING `{project_id}.{dataset_name}.temp_new_versions` S
            ON T.{key_col} = S.{key_col} AND T.is_active = TRUE
            WHEN MATCHED THEN
            UPDATE SET end_date = CURRENT_DATE(), is_active = FALSE
            WHEN NOT MATCHED THEN
            INSERT ({key_col}, {', '.join(attributes)}, start_date, end_date, is_active)
            VALUES (S.{key_col}, {', '.join(['S.' + attr for attr in attributes])}, CURRENT_DATE(), NULL, TRUE)
            """
            client.query(merge_query).result()


            print(f"Updated/inserted {len(new_versions)} changed records in {dim_table}")
        except GoogleAPIError as e:
            print(f"Error merging records into {dim_table}: {e}")
    else:
        print(f"No changed records for {dim_table}")