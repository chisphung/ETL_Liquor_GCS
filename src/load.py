from src.config import engine
import pandas as pd
from src.utils import process_scd_type2  # Assuming this utility function is defined in utils.py
# from src.transform import transform
import gc

def load(transformed_data):
    if not transformed_data:
        print("No data provided for loading.")
        return
    
    print("Starting load phase...")
    with engine.begin() as conn:  # Use transaction
        # Load dimension tables
        if not transformed_data['dates'].empty:
            transformed_data['dates'].to_sql('Date_Dim', conn, if_exists='append', index=False)
        
        if not transformed_data['stores'].empty:
            stores = transformed_data['stores'].rename(columns={'store': 'store_id'})
            process_scd_type2(stores, 'Store_Dim', 'store_id', 
                             ['address', 'city', 'zipcode', 'county_number', 'county'], conn)
        
        if not transformed_data['items'].empty:
            process_scd_type2(transformed_data['items'], 'Item_Dim', 'itemno', 
                             ['category', 'category_name', 'pack', 'bottle_volume_ml', 
                              'state_bottle_cost', 'state_bottle_retail'], conn)
        
        if not transformed_data['vendors'].empty:
            process_scd_type2(transformed_data['vendors'], 'Vendor_Dim', 'vendor_no', 
                             ['vendor_name'], conn)
        
        # Get keys for fact table and ensure proper data types
        date_keys = pd.read_sql("SELECT date, date_key FROM Date_Dim", conn)
        # Convert date column to datetime to match sales_data
        date_keys['date'] = pd.to_datetime(date_keys['date'])
        
        store_keys = pd.read_sql("SELECT store_id, store_key FROM Store_Dim WHERE is_active = 1", conn)
        # Ensure store_id is string type to match sales_data
        store_keys['store_id'] = store_keys['store_id'].astype(str)
        
        item_keys = pd.read_sql("SELECT itemno, item_key FROM Item_Dim WHERE is_active = 1", conn)
        # Ensure itemno is string type to match sales_data
        item_keys['itemno'] = item_keys['itemno'].astype(str)
        
        vendor_keys = pd.read_sql("SELECT vendor_no, vendor_key FROM Vendor_Dim WHERE is_active = 1", conn)
        # Ensure vendor_no is string type to match sales_data
        vendor_keys['vendor_no'] = vendor_keys['vendor_no'].astype(str)
        
        # Prepare Sales_Fact data
        sales_data = transformed_data['sales'].copy()
        
        # Select only necessary columns early to reduce memory
        necessary_columns = [
            'invoice_line_no', 'store', 'date', 'itemno', 'vendor_no',
            'revenue', 'profit', 'cost', 'total_bottles_sold', 'total_volume_sold_in_liters',
            'profit_margin', 'average_bottle_price', 'volume_per_bottle_sold', 'processed_timestamp'
        ]
        sales_data = sales_data[[col for col in necessary_columns if col in sales_data.columns]]
        
        # Process in smaller chunks to avoid memory issues
        chunk_size = 1000  # Restored to reasonable size since we're fixing the real issue
        sales_fact_chunks = []
        
        for start in range(0, len(sales_data), chunk_size):
            end = min(start + chunk_size, len(sales_data))
            chunk = sales_data.iloc[start:end].copy()
            print(f"Processing chunk {start // chunk_size + 1} ({len(chunk)} rows)...")

            if len(chunk) > chunk_size * 2:
                print(f"WARNING: Chunk size unexpectedly large: {len(chunk)} rows. Skipping.")
                del chunk
                gc.collect()
                continue

            # Ensure consistent data types in chunk
            chunk['date'] = pd.to_datetime(chunk['date'])
            chunk['store'] = chunk['store'].astype(str)
            chunk['itemno'] = chunk['itemno'].astype(str)
            chunk['vendor_no'] = chunk['vendor_no'].astype(str)

            print(f"  Actual chunk size before merge: {len(chunk)} rows")

            try:
                # Filter dimension tables
                chunk_dates = chunk['date'].unique()
                chunk_stores = chunk['store'].unique()
                chunk_items = chunk['itemno'].unique()
                chunk_vendors = chunk['vendor_no'].unique()

                print(f"  Unique dates: {len(chunk_dates)}, stores: {len(chunk_stores)}, items: {len(chunk_items)}, vendors: {len(chunk_vendors)}")

                filtered_date_keys = date_keys[date_keys['date'].isin(chunk_dates)][['date', 'date_key']].copy()
                filtered_store_keys = store_keys[store_keys['store_id'].isin(chunk_stores)][['store_id', 'store_key']].copy()
                filtered_item_keys = item_keys[item_keys['itemno'].isin(chunk_items)][['itemno', 'item_key']].copy()
                filtered_vendor_keys = vendor_keys[vendor_keys['vendor_no'].isin(chunk_vendors)][['vendor_no', 'vendor_key']].copy()

                print(f"  Filtered keys - dates: {len(filtered_date_keys)}, stores: {len(filtered_store_keys)}, items: {len(filtered_item_keys)}, vendors: {len(filtered_vendor_keys)}")

                # Validate filtered date_keys size
                if len(filtered_date_keys) > len(chunk_dates):
                    print(f"ERROR: filtered_date_keys has {len(filtered_date_keys)} rows for {len(chunk_dates)} unique dates!")
                    raise ValueError("Duplicate dates detected in date_keys.")

                # Perform merges with validation
                chunk_merged = chunk.merge(filtered_date_keys, on='date', how='left')
                print(f"  After date merge: {len(chunk_merged)} rows")
                if len(chunk_merged) > len(chunk):
                    print(f"ERROR: Row count increased from {len(chunk)} to {len(chunk_merged)} after date merge!")
                    print(f"Unmatched dates: {chunk_merged[chunk_merged['date_key'].isna()]['date'].unique()}")
                    raise ValueError("Unexpected row increase after date merge.")

                chunk_merged = chunk_merged.merge(filtered_store_keys, left_on='store', right_on='store_id', how='left')
                print(f"  After store merge: {len(chunk_merged)} rows")
                if len(chunk_merged) > len(chunk):
                    raise ValueError("Unexpected row increase after store merge.")

                chunk_merged = chunk_merged.merge(filtered_item_keys, on='itemno', how='left')
                print(f"  After item merge: {len(chunk_merged)} rows")
                if len(chunk_merged) > len(chunk):
                    raise ValueError("Unexpected row increase after item merge.")

                chunk_merged = chunk_merged.merge(filtered_vendor_keys, on='vendor_no', how='left')
                print(f"  After vendor merge: {len(chunk_merged)} rows")
                if len(chunk_merged) > len(chunk):
                    raise ValueError("Unexpected row increase after vendor merge.")

                del chunk
                chunk = chunk_merged
                del chunk_merged

                # Clean up filtered dataframes
                del filtered_date_keys, filtered_store_keys, filtered_item_keys, filtered_vendor_keys
                del chunk_dates, chunk_stores, chunk_items, chunk_vendors
                gc.collect()

            except Exception as e:
                print(f"Error during merge operations: {e}")
                print(f"chunk size: {len(chunk) if 'chunk' in locals() else 'N/A'} rows")
                print(f"date_keys size: {len(date_keys)} rows")
                print(f"store_keys size: {len(store_keys)} rows")
                print(f"item_keys size: {len(item_keys)} rows")
                print(f"vendor_keys size: {len(vendor_keys)} rows")
                if 'chunk' in locals():
                    print(f"chunk['date'].dtype: {chunk.get('date', pd.Series()).dtype}")
                    print(f"chunk['store'].dtype: {chunk.get('store', pd.Series()).dtype}")
                    print(f"chunk['itemno'].dtype: {chunk.get('itemno', pd.Series()).dtype}")
                    print(f"chunk['vendor_no'].dtype: {chunk.get('vendor_no', pd.Series()).dtype}")
                del chunk
                gc.collect()
                continue

            # Select final columns
            chunk_fact = chunk[[
                'invoice_line_no', 'store', 'date_key', 'store_key', 'item_key', 'vendor_key',
                'revenue', 'profit', 'cost', 'total_bottles_sold', 'total_volume_sold_in_liters',
                'profit_margin', 'average_bottle_price', 'volume_per_bottle_sold', 'processed_timestamp'
            ]].dropna(subset=['date_key', 'store_key', 'item_key', 'vendor_key'])

            if not chunk_fact.empty:
                sales_fact_chunks.append(chunk_fact)
                print(f"Chunk {start // chunk_size + 1} processed successfully: {len(chunk_fact)} valid records")
            else:
                print(f"Chunk {start // chunk_size + 1} contained no valid records after merging")

            del chunk, chunk_fact
            gc.collect()

        # Process chunks in batches for database loading
        if sales_fact_chunks:
            batch_size = 3
            total_loaded = 0

            for i in range(0, len(sales_fact_chunks), batch_size):
                batch_chunks = sales_fact_chunks[i:i + batch_size]
                if batch_chunks:
                    sales_fact_batch = pd.concat(batch_chunks, ignore_index=True)
                    if not sales_fact_batch.empty:
                        sales_fact_batch.to_sql('Sales_Fact', conn, if_exists='append', index=False, method='multi')
                        total_loaded += len(sales_fact_batch)
                        print(f"Loaded batch {i//batch_size + 1}: {len(sales_fact_batch)} records")
                    del sales_fact_batch, batch_chunks
                    gc.collect()

            print(f"Total loaded: {total_loaded} records into Sales_Fact table.")
            del sales_fact_chunks
            gc.collect()
        else:
            print("No chunks processed.")
            