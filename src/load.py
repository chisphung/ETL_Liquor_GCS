import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from src.common import UserCredentials
from src.utils import process_scd_type2
from src.config import PROJECT_ID, DATASET_ID
import gc

def load(transformed_data, dataset_name=DATASET_ID):
    if not transformed_data:
        print("No data provided for loading.")
        return
    
    print("Starting load phase...")
    
    # Initialize credentials and BigQuery client
    conn = bigquery.Client()
    
    # Load dimension tables
    if not transformed_data['dates'].empty:
        pandas_gbq.to_gbq(
            transformed_data['dates'],
            f'{dataset_name}.Date_Dim',
            project_id=PROJECT_ID,
            if_exists='append'
        )
        print("Loaded Date_Dim")
    
    if not transformed_data['stores'].empty:
        transformed_data['stores'] = transformed_data['stores'].astype({'store': int})
        stores = transformed_data['stores'].rename(columns={'store': 'store_id'})
        # process_scd_type2(stores, f'{dataset_name}.Store_Dim', 'store_id', 
        #                 ['address', 'city', 'zipcode', 'county_number', 'county'], project_id=PROJECT_ID)
        process_scd_type2(stores, f'Store_Dim', 'store_id',
                         ['address', 'city', 'zipcode', 'county_number', 'county'])
        print("Loaded Store_Dim")
    
# CREATE TABLE `smooth-hub-460704-v8.liquor_sale_.Item_Dim` (
#   item_key INT64,
#   itemno STRING NOT NULL,
#   im_desc STRING,
#   category STRING,
#   category_name STRING,
#   pack FLOAT64,
#   bottle_volume_ml FLOAT64,
#   state_bottle_cost NUMERIC(10, 2),
#   state_bottle_retail NUMERIC(10, 2),
#   start_date DATE NOT NULL,
#   end_date DATE,
#   is_active BOOL DEFAULT TRUE
# );
    if not transformed_data['items'].empty:
        # transformed_data['items'] = transformed_data['items'].astype({'itemno': str, 'bottle_volume_ml': float, 'state_bottle_cost': float, 'state_bottle_retail': float, 'pack': float})
        # process_scd_type2(transformed_data['items'], f'Item_Dim', 'itemno',
        #                  ['category', 'category_name', 'pack', 'bottle_volume_ml',
        #                   'state_bottle_cost', 'state_bottle_retail'])
        # print("Loaded Item_Dim")
        items_df = transformed_data['items'].copy()
        items_df['itemno'] = items_df['itemno'].astype(str)
        items_df['bottle_volume_ml'] = items_df['bottle_volume_ml'].astype(float)
        items_df['state_bottle_cost'] = items_df['state_bottle_cost'].astype(float).round(2)
        items_df['state_bottle_retail'] = items_df['state_bottle_retail'].astype(float).round(2)
        items_df['pack'] = items_df['pack'].astype(float)

        process_scd_type2(items_df, f'Item_Dim', 'itemno',
            ['category', 'category_name', 'pack', 'bottle_volume_ml',
            'state_bottle_cost', 'state_bottle_retail'])
        print("Loaded Item_Dim")

    
    if not transformed_data['vendors'].empty:
        transformed_data['vendors'] = transformed_data['vendors'].astype({'vendor_no': str})
        process_scd_type2(transformed_data['vendors'], f'Vendor_Dim', 'vendor_no',
                         ['vendor_name'])
        print("Loaded Vendor_Dim")
    
    # Cache dimension keys
    date_keys = pandas_gbq.read_gbq(f"SELECT date, date_key FROM {dataset_name}.Date_Dim", 
                           project_id=PROJECT_ID)
    date_keys['date'] = pd.to_datetime(date_keys['date'])

    store_keys = pandas_gbq.read_gbq(f"SELECT store_id, store_key FROM {dataset_name}.Store_Dim WHERE is_active = 1", 
                            project_id=PROJECT_ID)
    store_keys['store_id'] = store_keys['store_id'].astype(int)

    item_keys = pandas_gbq.read_gbq(f"SELECT itemno, item_key FROM {dataset_name}.Item_Dim WHERE is_active = 1", 
                           project_id=PROJECT_ID)
    item_keys['itemno'] = item_keys['itemno'].astype(str)

    vendor_keys = pandas_gbq.read_gbq(f"SELECT vendor_no, vendor_key FROM {dataset_name}.Vendor_Dim WHERE is_active = 1", 
                             project_id=PROJECT_ID)
    vendor_keys['vendor_no'] = vendor_keys['vendor_no'].astype(str)
    
    # Prepare Sales_Fact data
    sales_data = transformed_data['sales'].copy()
    
    # Validate required columns
    required_columns = ['invoice_line_no', 'store', 'date', 'itemno', 'vendor_no']
    missing_cols = [col for col in required_columns if col not in sales_data.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    necessary_columns = [
        'invoice_line_no', 'store', 'date', 'itemno', 'vendor_no',
        'revenue', 'profit', 'cost', 'total_bottles_sold', 'total_volume_sold_in_liters',
        'profit_margin', 'average_bottle_price', 'volume_per_bottle_sold', 'processed_timestamp'
    ]
    sales_data = sales_data[[col for col in necessary_columns if col in sales_data.columns]]
    
    # Process in chunks
    chunk_size = 1000
    sales_fact_chunks = []
    failed_chunks = []
    
    for start in range(0, len(sales_data), chunk_size):
        end = min(start + chunk_size, len(sales_data))
        chunk = sales_data.iloc[start:end].copy()
        print(f"Processing chunk {start // chunk_size + 1} ({len(chunk)} rows)...")
        
        # Ensure consistent data types
        chunk['date'] = pd.to_datetime(chunk['date'])
        chunk['store'] = chunk['store'].astype(str)
        chunk['itemno'] = chunk['itemno'].astype(str)
        chunk['vendor_no'] = chunk['vendor_no'].astype(str)
        
        try:
            # Filter dimension keys
            chunk_dates = chunk['date'].unique()
            chunk_stores = chunk['store'].unique()
            chunk_items = chunk['itemno'].unique()
            chunk_vendors = chunk['vendor_no'].unique()
            
            filtered_date_keys = date_keys[date_keys['date'].isin(chunk_dates)][['date', 'date_key']].copy()
            filtered_store_keys = store_keys[store_keys['store_id'].isin(chunk_stores)][['store_id', 'store_key']].copy()
            filtered_item_keys = item_keys[item_keys['itemno'].isin(chunk_items)][['itemno', 'item_key']].copy()
            filtered_vendor_keys = vendor_keys[vendor_keys['vendor_no'].isin(chunk_vendors)][['vendor_no', 'vendor_key']].copy()
            
            # Validate merge sizes
            chunk_merged = chunk.merge(filtered_date_keys, on='date', how='left')
            if len(chunk_merged) > len(chunk):
                raise ValueError("Unexpected row increase after date merge.")
            
            chunk_merged = chunk_merged.merge(filtered_store_keys, left_on='store', right_on='store_id', how='left')
            if len(chunk_merged) > len(chunk):
                raise ValueError("Unexpected row increase after store merge.")
            
            chunk_merged = chunk_merged.merge(filtered_item_keys, on='itemno', how='left')
            if len(chunk_merged) > len(chunk):
                raise ValueError("Unexpected row increase after item merge.")
            
            chunk_merged = chunk_merged.merge(filtered_vendor_keys, on='vendor_no', how='left')
            if len(chunk_merged) > len(chunk):
                raise ValueError("Unexpected row increase after vendor merge.")
            
            # Check for missing keys
            missing_keys = chunk_merged[chunk_merged[['date_key', 'store_key', 'item_key', 'vendor_key']].isna().any(axis=1)]
            if not missing_keys.empty:
                print(f"Warning: {len(missing_keys)} rows dropped due to missing keys in chunk {start // chunk_size + 1}")
                missing_keys.to_csv(f'missing_keys_chunk_{start // chunk_size + 1}.csv')
            
            # Select final columns
            chunk_fact = chunk_merged[[
                'invoice_line_no', 'store', 'date_key', 'store_key', 'item_key', 'vendor_key',
                'revenue', 'profit', 'cost', 'total_bottles_sold', 'total_volume_sold_in_liters',
                'profit_margin', 'average_bottle_price', 'volume_per_bottle_sold', 'processed_timestamp'
            ]].dropna(subset=['date_key', 'store_key', 'item_key', 'vendor_key'])
            
            if not chunk_fact.empty:
                sales_fact_chunks.append(chunk_fact)
                print(f"Chunk {start // chunk_size + 1} processed successfully: {len(chunk_fact)} valid records")
            
            del chunk, chunk_merged, filtered_date_keys, filtered_store_keys, filtered_item_keys, filtered_vendor_keys
            gc.collect()
        
        except Exception as e:
            print(f"Error in chunk {start // chunk_size + 1}: {e}")
            failed_chunks.append((start, chunk.copy()))
            del chunk
            gc.collect()
            continue
    
    # Save failed chunks
    if failed_chunks:
        pd.DataFrame(failed_chunks, columns=['start_index', 'data']).to_csv('failed_chunks.csv')
        print(f"Saved {len(failed_chunks)} failed chunks to 'failed_chunks.csv'")
    
    # Load chunks to Sales_Fact
    if sales_fact_chunks:
        batch_size = 10000  # Larger batch size for BigQuery
        total_loaded = 0
        
        for i in range(0, len(sales_fact_chunks), batch_size // chunk_size):
            batch_chunks = sales_fact_chunks[i:i + (batch_size // chunk_size)]
            if batch_chunks:
                sales_fact_batch = pd.concat(batch_chunks, ignore_index=True)
                if not sales_fact_batch.empty:
                    pandas_gbq.to_gbq(
                        sales_fact_batch,
                        f'{dataset_name}.Sales_Fact',
                        project_id=PROJECT_ID,
                        if_exists='append'
                    )
                    total_loaded += len(sales_fact_batch)
                    print(f"Loaded batch {i // (batch_size // chunk_size) + 1}: {len(sales_fact_batch)} records")
                del sales_fact_batch, batch_chunks
                gc.collect()
        
        print(f"Total loaded: {total_loaded} records into Sales_Fact table.")
        del sales_fact_chunks
        gc.collect()
    else:
        print("No chunks processed.")