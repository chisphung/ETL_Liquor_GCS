from src.config import engine
import pandas as pd
from google.cloud import bigquery
from src.config import PROJECT_ID, DATASET_ID
import pandas_gbq

def transform():
    print("Starting transform phase...")
    
    # Get all unprocessed records from staging
    staging_query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.Staging_Sales`
        WHERE processed_timestamp > COALESCE((SELECT MAX(processed_timestamp) FROM `{PROJECT_ID}.{DATASET_ID}.Sales_Fact`), '1900-01-01')
    """
    
    try:
        # staging_data = pd.read_sql(staging_query, engine)
        staging_data = pandas_gbq.read_gbq(staging_query, project_id=PROJECT_ID)
    except:
        # First run - get all data
        # staging_data = pd.read_sql("SELECT * FROM Staging_Sales", engine)
        staging_data = pandas_gbq.read_gbq("SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.Staging_Sales`", project_id=PROJECT_ID)

    if staging_data.empty:
        print("No new data to transform.")
        return None
    
    # Data cleaning
    ## Duplicated
    staging_data = staging_data[staging_data.duplicated(subset=['invoice_line_no', 'store'], keep='first') | ~staging_data.duplicated(subset=['invoice_line_no', 'store'], keep=False)]
    
    ## NUll
    staging_data.drop(columns=["store_location"], inplace=True)
    # staging_data = staging_data.dropna(subset=['state_bottle_cost', 'state_bottle_retail', 'sale_bottles', 'sale_dollars', 'sale_liters', 'sale_gallons'])
    staging_data.fillna(value={'address': 'Unknown',
                    'city': 'Unknown',
                    'zipcode': 'Unknown',
                    'county_number': 'Unknown',
                    'county': 'Unknown',
                    'category': 'Unknown',
                    'category_name': 'Unknown'}, inplace=True)    
    staging_data = staging_data.dropna()
    ## Type Casting
    staging_data['date'] = pd.to_datetime(staging_data['date'], errors='coerce')
    numeric_cols = ['state_bottle_cost', 'state_bottle_retail', 'sale_bottles', 
                  'sale_dollars', 'sale_liters', 'sale_gallons']
    for col in numeric_cols:
        staging_data[col] = pd.to_numeric(staging_data[col], errors='coerce')
    
    # Calculate metrics
    staging_data['revenue'] = staging_data['sale_dollars']
    staging_data['cost'] = staging_data['state_bottle_cost'] * staging_data['sale_bottles']
    staging_data['profit'] = (staging_data['state_bottle_retail'] - staging_data['state_bottle_cost']) * staging_data['sale_bottles']
    staging_data['total_bottles_sold'] = staging_data['sale_bottles']
    staging_data['total_volume_sold_in_liters'] = staging_data['sale_liters']
    staging_data['profit_margin'] = (staging_data['profit'] / staging_data['revenue'] * 100).round(2).where(staging_data['revenue'] > 0, 0)
    staging_data['average_bottle_price'] = (staging_data['sale_dollars'] / staging_data['sale_bottles']).round(2).where(staging_data['sale_bottles'] > 0, 0)
    staging_data['volume_per_bottle_sold'] = (staging_data['sale_liters'] / staging_data['sale_bottles']).round(2).where(staging_data['sale_bottles'] > 0, 0)
    
    # Prepare dimension data
    # transformed = {
    #     'dates': staging_data[['date']],
    #     'stores': staging_data[['store', 'address', 'city', 'zipcode', 'county_number', 'county']],
    #     'items': staging_data[['itemno', 'im_desc', 'category', 'category_name', 'pack', 'bottle_volume_ml', 'state_bottle_cost', 'state_bottle_retail']],
    #     'vendors': staging_data[['vendor_no', 'vendor_name']],
    #     'sales': staging_data
    # }
    transformed = {
    'dates': staging_data[['date']].copy(),
    'stores': staging_data[['store', 'address', 'city', 'zipcode', 'county_number', 'county']].copy(),
    'items': staging_data[['itemno', 'im_desc', 'category', 'category_name', 'pack', 'bottle_volume_ml', 'state_bottle_cost', 'state_bottle_retail']].copy(),
    'vendors': staging_data[['vendor_no', 'vendor_name']].copy(),
    'sales': staging_data.copy()
    }
    
    # Prepare date dimension attributes
    transformed['dates']['date'] = pd.to_datetime(transformed['dates']['date'])
    transformed['dates'].drop_duplicates(subset=['date'], keep='last', inplace=True)
    transformed['dates']['year'] = transformed['dates']['date'].dt.year
    transformed['dates']['month'] = transformed['dates']['date'].dt.month
    transformed['dates']['day'] = transformed['dates']['date'].dt.day
    transformed['dates']['quarter'] = transformed['dates']['date'].dt.quarter
    transformed['dates']['weekday'] = transformed['dates']['date'].dt.day_name()
    
    transformed['stores'].drop_duplicates(subset=['store'], keep='last', inplace=True)
    transformed['items'].drop_duplicates(subset=['itemno'], keep='last', inplace=True)
    transformed['vendors'].drop_duplicates(subset=['vendor_no'], keep='last', inplace=True)
    
    
    print(f"Transformed {len(staging_data)} records for loading.")
    return transformed