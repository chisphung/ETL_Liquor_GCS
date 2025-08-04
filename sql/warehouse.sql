-- Drop tables if they exist (BigQuery supports this)
DROP TABLE IF EXISTS `smooth-hub-460704-v8.liquor_sale_.Date_Dim`;
DROP TABLE IF EXISTS `smooth-hub-460704-v8.liquor_sale_.Store_Dim`;
DROP TABLE IF EXISTS `smooth-hub-460704-v8.liquor_sale_.Item_Dim`;
DROP TABLE IF EXISTS `smooth-hub-460704-v8.liquor_sale_.Vendor_Dim`;
DROP TABLE IF EXISTS `smooth-hub-460704-v8.liquor_sale_.Sales_Fact`;

-- Date Dimension
CREATE TABLE `smooth-hub-460704-v8.liquor_sale_.Date_Dim` (
  date_key INT64,
  date DATE NOT NULL,
  year INT64,
  month INT64,
  day INT64,
  quarter INT64,
  weekday STRING
);

-- Store Dimension (SCD Type 2)
CREATE TABLE `smooth-hub-460704-v8.liquor_sale_.Store_Dim` (
  store_key INT64,
  store_id INT64 NOT NULL,
  address STRING,
  city STRING,
  zipcode STRING,
  county_number STRING,
  county STRING,
  start_date DATE NOT NULL,
  end_date DATE,
  is_active BOOL DEFAULT TRUE
);

-- Item Dimension (SCD Type 2)
CREATE TABLE `smooth-hub-460704-v8.liquor_sale_.Item_Dim` (
  item_key INT64,
  itemno STRING NOT NULL,
  im_desc STRING,
  category STRING,
  category_name STRING,
  pack FLOAT64,
  bottle_volume_ml FLOAT64,
  state_bottle_cost NUMERIC(10, 2),
  state_bottle_retail NUMERIC(10, 2),
  start_date DATE NOT NULL,
  end_date DATE,
  is_active BOOL DEFAULT TRUE
);

-- Vendor Dimension (SCD Type 2)
CREATE TABLE `smooth-hub-460704-v8.liquor_sale_.Vendor_Dim` (
  vendor_key INT64,
  vendor_no STRING NOT NULL,
  vendor_name STRING,
  start_date DATE NOT NULL,
  end_date DATE,
  is_active BOOL DEFAULT TRUE
);

-- Sales Fact Table
CREATE TABLE `smooth-hub-460704-v8.liquor_sale_.Sales_Fact` (
  sales_key INT64,
  invoice_line_no STRING NOT NULL,
  store INT64 NOT NULL,
  date_key INT64 NOT NULL,
  store_key INT64 NOT NULL,
  item_key INT64 NOT NULL,
  vendor_key INT64 NOT NULL,
  revenue NUMERIC(10, 2),
  profit NUMERIC(10, 2),
  cost NUMERIC(10, 2),
  total_bottles_sold INT64,
  total_volume_sold_in_liters NUMERIC(10, 2),
  profit_margin NUMERIC(5, 2),
  average_bottle_price NUMERIC(10, 2),
  volume_per_bottle_sold NUMERIC(10, 2),
  processed_timestamp DATETIME
  -- Foreign keys not enforced in BigQuery, just for documentation
);

-- Indexes → BigQuery does not support CREATE INDEX
-- Instead, use clustering or partitioning if needed

-- Example clustering (optional)
-- CREATE TABLE `smooth-hub-460704-v8.liquor_sale_.Sales_Fact`
-- CLUSTER BY date_key, store_key, item_key, vendor_key
