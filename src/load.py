import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from src.common import UserCredentials
from src.utils import process_scd_type2
from src.config import PROJECT_ID, DATASET_ID
import gc
from decimal import Decimal, ROUND_HALF_UP
import numpy as np

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
        transformed_data['stores'] = transformed_data['stores'].astype({
            'store': int, 'address': str, 'city': str, 'zipcode': str, 
            'county_number': str, 'county': str
        })
        stores = transformed_data['stores'].rename(columns={'store': 'store_id'})
        process_scd_type2(stores, f'Store_Dim', 'store_id',
                         ['address', 'city', 'zipcode', 'county_number', 'county'])
        print("Loaded Store_Dim")
    
    if not transformed_data['items'].empty:
        items_df = transformed_data['items'].copy()
        items_df['itemno'] = items_df['itemno'].astype(str)
        items_df['bottle_volume_ml'] = items_df['bottle_volume_ml'].astype(float).round(2)
        items_df['pack'] = items_df['pack'].astype(float).round(2)
        
        # Check for non-numeric or NaN values in numeric columns
        for col in ['state_bottle_cost', 'state_bottle_retail']:
            invalid_rows = items_df[items_df[col].isna() | ~items_df[col].apply(
                lambda x: isinstance(x, (int, float, str)) and str(x).replace('.', '', 1).replace('-', '', 1).isdigit()
            )]
            if not invalid_rows.empty:
                print(f"Warning: Invalid values found in {col}:")
                print(invalid_rows[[col]])
                items_df[col] = pd.to_numeric(items_df[col], errors='coerce').fillna(-1).astype(float)
        
        # Convert to Decimal with 2 decimal places to match NUMERIC(10, 2)
        for col in ['state_bottle_cost', 'state_bottle_retail']:
            items_df[col] = items_df[col].astype(float).apply(
                lambda x: Decimal(str(round(x, 2))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) 
                if not pd.isna(x) else Decimal('0.00')
            )
        
        # Validate data to ensure it fits NUMERIC(10, 2)
        for col in ['state_bottle_cost', 'state_bottle_retail']:
            invalid_rows = items_df[items_df[col].apply(lambda x: abs(x) > Decimal('99999999.99'))]
            if not invalid_rows.empty:
                print(f"Warning: Values in {col} exceed NUMERIC(10, 2) limits:")
                print(invalid_rows[[col]])
                raise ValueError(f"Data in {col} contains values exceeding NUMERIC(10, 2) limits")
        
        print(f"Processing Item_dim:\n")
        process_scd_type2(items_df, 'Item_Dim', 'itemno',
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
    
    store_keys = pandas_gbq.read_gbq(f"SELECT store_id, store_key FROM {dataset_name}.Store_Dim WHERE is_active = TRUE", 
                                     project_id=PROJECT_ID)
    store_keys['store_id'] = store_keys['store_id'].fillna(-1).astype(int)  # Fill NaN with -1 for inactive stores
    store_keys['store_id'] = store_keys['store_id'].astype(int)
    store_keys['store_key'] = store_keys['store_key'].fillna(-1).astype(int)  # Fill NaN with -1 for inactive stores
    store_keys['store_key'] = store_keys['store_key'].astype(int)  # Ensure store_key is int
    
    item_keys = pandas_gbq.read_gbq(f"SELECT itemno, item_key FROM {dataset_name}.Item_Dim WHERE is_active = TRUE", 
                                    project_id=PROJECT_ID)
    item_keys['itemno'] = item_keys['itemno'].fillna('Unknown').astype(str)  # Fill NaN with 'Unknown'
    item_keys['item_key'] = item_keys['item_key'].fillna(-1).astype(int)
    item_keys['itemno'] = item_keys['itemno'].astype(str)
    item_keys['item_key'] = item_keys['item_key'].astype(int)  # Ensure item_key is int
    
    vendor_keys = pandas_gbq.read_gbq(f"SELECT vendor_no, vendor_key FROM {dataset_name}.Vendor_Dim WHERE is_active = TRUE", 
                                      project_id=PROJECT_ID)
    vendor_keys['vendor_no'] = vendor_keys['vendor_no'].fillna('Unknown').astype(str)  # Fill NaN with 'Unknown'
    vendor_keys['vendor_key'] = vendor_keys['vendor_key'].fillna(-1).astype(int)
    vendor_keys['vendor_no'] = vendor_keys['vendor_no'].astype(str)
    vendor_keys['vendor_key'] = vendor_keys['vendor_key'].astype(int)  # Ensure vendor_key is int
    
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
    
    date_keys = date_keys.drop_duplicates(subset=['date'], keep='last')
    date_keys['date'] = pd.to_datetime(date_keys['date'], errors='coerce')
    date_keys = date_keys.set_index('date')
    
    for start in range(0, len(sales_data), chunk_size):
        end = min(start + chunk_size, len(sales_data))
        chunk = sales_data.iloc[start:end].copy()  # Avoid SettingWithCopyWarning
        print(f"Processing chunk {start // chunk_size + 1} ({len(chunk)} rows)...")
        
        # Ensure consistent data types for INTEGER columns
        integer_cols = ['store', 'itemno', 'vendor_no']
        for col in integer_cols:
            invalid_rows = chunk[~chunk[col].apply(
                lambda x: str(x).replace('-', '').isdigit() if pd.notna(x) else False
            )]
            if not invalid_rows.empty:
                print(f"Warning: Invalid values in {col}:")
                print(invalid_rows[[col]])
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(-1).astype(int)
        
        # Convert date column
        chunk['date'] = pd.to_datetime(chunk['date'], errors='coerce')
        
        # Convert numeric columns to Decimal for NUMERIC(10, 2) and NUMERIC(5, 2)
        numeric_cols = [
            'revenue', 'profit', 'cost', 'total_volume_sold_in_liters',
            'average_bottle_price', 'volume_per_bottle_sold'
        ]
        for col in numeric_cols:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(-1).astype(float).apply(
                    lambda x: Decimal(str(round(x, 2))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) 
                    if not pd.isna(x) else Decimal('0.00')
                )
        
        if 'profit_margin' in chunk.columns:
            # chunk['profit_margin'] = pd.to_numeric(chunk['profit_margin'], errors='coerce').fillna(-1).astype(float).apply(
            #     lambda x: Decimal(str(round(x, 2))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) 
            #     if not pd.isna(x) else Decimal('0.00')
            # )
            numeric_profit_margin = pd.to_numeric(chunk['profit_margin'], errors='coerce').fillna(-1).astype(float)
    
            clamped_profit_margin = np.clip(numeric_profit_margin, -999.99, 999.99)
            chunk['profit_margin'] = pd.Series(clamped_profit_margin, index=chunk.index).apply(
                lambda x: Decimal(str(round(x, 2))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) 
                if not pd.isna(x) else Decimal('0.00')
            )
        # Ensure total_bottles_sold is INTEGER
        if 'total_bottles_sold' in chunk.columns:
            chunk['total_bottles_sold'] = pd.to_numeric(chunk['total_bottles_sold'], errors='coerce').fillna(-1).astype(int)
        
        # Add processed_timestamp
        chunk['processed_timestamp'] = pd.Timestamp.now()
        
        # Drop rows with NaT in date
        if chunk['date'].isna().any():
            print(f"Warning: Dropping {chunk['date'].isna().sum()} rows with NaT in 'date' for chunk {start // chunk_size + 1}")
            chunk = chunk.dropna(subset=['date'])
        
        try:
            # Filter dimension keys
            chunk_dates = chunk['date'].unique()
            chunk_stores = chunk['store'].unique()
            chunk_items = chunk['itemno'].unique()
            chunk_vendors = chunk['vendor_no'].unique()
            
            # Filter and deduplicate date_keys
            filtered_date_keys = date_keys.loc[chunk_dates, ['date_key']].reset_index().drop_duplicates(subset=['date'], keep='first')
            filtered_store_keys = store_keys[store_keys['store_id'].isin(chunk_stores)][['store_id', 'store_key']]
            filtered_item_keys = item_keys[item_keys['itemno'].isin(chunk_items)][['itemno', 'item_key']]
            filtered_vendor_keys = vendor_keys[vendor_keys['vendor_no'].isin(chunk_vendors)][['vendor_no', 'vendor_key']]
            
            # Merge with date_keys
            chunk_merged = chunk.merge(filtered_date_keys, on='date', how='left')
            if len(chunk_merged) > len(chunk):
                chunk.to_csv(f'chunk_before_merge_{start // chunk_size + 1}.csv', index=False)
                chunk_merged.to_csv(f'chunk_after_merge_{start // chunk_size + 1}.csv', index=False)
                raise ValueError(f"Unexpected row increase after date merge in chunk {start // chunk_size + 1}. Saved data for debugging.")
            
            # Set default for missing date_key and ensure INTEGER
            chunk_merged['date_key'] = pd.to_numeric(chunk_merged['date_key'], errors='coerce').fillna(-1).astype(int)
            
            # Merge with store_keys and drop store_id to avoid duplicate columns
            chunk_merged = chunk_merged.merge(filtered_store_keys, left_on='store', right_on='store_id', how='left').drop(columns=['store_id'], errors='ignore')
            if len(chunk_merged) > len(chunk):
                raise ValueError("Unexpected row increase after store merge.")
            chunk_merged['store_key'] = pd.to_numeric(chunk_merged['store_key'], errors='coerce').fillna(-1).astype(int)
            
            # Merge with item_keys
            chunk_merged = chunk_merged.merge(filtered_item_keys, on='itemno', how='left')
            if len(chunk_merged) > len(chunk):
                raise ValueError("Unexpected row increase after item merge.")
            chunk_merged['item_key'] = pd.to_numeric(chunk_merged['item_key'], errors='coerce').fillna(-1).astype(int)
            
            # Merge with vendor_keys
            chunk_merged = chunk_merged.merge(filtered_vendor_keys, on='vendor_no', how='left')
            if len(chunk_merged) > len(chunk):
                raise ValueError("Unexpected row increase after vendor merge.")
            chunk_merged['vendor_key'] = pd.to_numeric(chunk_merged['vendor_key'], errors='coerce').fillna(-1).astype(int)
            
            # Debug: Check for string values in INTEGER columns
            integer_cols_final = ['store', 'date_key', 'store_key', 'item_key', 'vendor_key', 'total_bottles_sold']
            for col in integer_cols_final:
                if col in chunk_merged.columns and chunk_merged[col].apply(lambda x: isinstance(x, str)).any():
                    print(f"Warning: String values found in {col}:")
                    print(chunk_merged[chunk_merged[col].apply(lambda x: isinstance(x, str))][[col]])
                    raise ValueError(f"String values in {col} detected before loading.")
            
            # Select final columns
            required_columns = [
                'invoice_line_no', 'store', 'date_key', 'store_key', 'item_key', 'vendor_key',
                'revenue', 'profit', 'cost', 'total_bottles_sold', 'total_volume_sold_in_liters',
                'profit_margin', 'average_bottle_price', 'volume_per_bottle_sold', 'processed_timestamp'
            ]
            missing_cols = [col for col in required_columns if col not in chunk_merged.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns in chunk {start // chunk_size + 1}: {missing_cols}")
            
            chunk_fact = chunk_merged[required_columns].dropna(subset=['date_key', 'store_key', 'item_key', 'vendor_key'])
            if not chunk_fact.empty:
                # Final type check before loading
                print(f"Chunk {start // chunk_size + 1} dtypes:\n{chunk_fact.dtypes}")
                sales_fact_chunks.append(chunk_fact)
                print(f"Chunk {start // chunk_size + 1} processed successfully: {len(chunk_fact)} valid records")
        
        except ValueError as ve:
            print(f"ValueError in chunk {start // chunk_size + 1}: {ve}")
            chunk.to_csv(f'failed_chunk_{start // chunk_size + 1}.csv', index=False)
            failed_chunks.append({'start_index': start, 'data': chunk})
        except MemoryError as me:
            print(f"MemoryError in chunk {start // chunk_size + 1}: {me}")
            chunk.to_csv(f'failed_chunk_{start // chunk_size + 1}.csv', index=False)
            failed_chunks.append({'start_index': start, 'data': chunk})
        except Exception as e:
            print(f"Unexpected error in chunk {start // chunk_size + 1}: {e}")
            chunk.to_csv(f'failed_chunk_{start // chunk_size + 1}.csv', index=False)
            failed_chunks.append({'start_index': start, 'data': chunk})
        finally:
            del chunk, chunk_merged, filtered_date_keys, filtered_store_keys, filtered_item_keys, filtered_vendor_keys
            gc.collect()
    
    # Save failed chunks
    if failed_chunks:
        pd.DataFrame(failed_chunks, columns=['start_index', 'data']).to_csv('failed_chunks.csv')
        print(f"Saved {len(failed_chunks)} failed chunks to 'failed_chunks.csv'")
    
    # Load chunks to Sales_Fact
    print("Loading Sales_Fact...")
    if sales_fact_chunks:
        batch_size = 10000  # Larger batch size for BigQuery
        total_loaded = 0
        
        for i in range(0, len(sales_fact_chunks), batch_size // chunk_size):
            batch_chunks = sales_fact_chunks[i:i + (batch_size // chunk_size)]
            if batch_chunks:
                sales_fact_batch = pd.concat(batch_chunks, ignore_index=True)
                if not sales_fact_batch.empty:
                    # Debug: Check for '4849' in INTEGER columns
                    for col in ['store', 'date_key', 'store_key', 'item_key', 'vendor_key', 'total_bottles_sold']:
                        if col in sales_fact_batch.columns and sales_fact_batch[col].astype(str).str.contains('4849').any():
                            print(f"Value '4849' found in {col}:\n{sales_fact_batch[sales_fact_batch[col].astype(str).str.contains('4849')][[col]]}")
                    
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