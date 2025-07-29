DROP TABLE IF EXISTS Date_Dim;
DROP TABLE IF EXISTS Store_Dim;
DROP TABLE IF EXISTS Item_Dim;
DROP TABLE IF EXISTS Vendor_Dim;
DROP TABLE IF EXISTS Sales_Fact;
-- Date Dimension
CREATE TABLE Date_Dim (
    date_key INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER,
    weekday TEXT
);
-- Store Dimension (SCD Type 2)
CREATE TABLE Store_Dim (
    store_key INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id INTEGER NOT NULL,
    address TEXT,
    city TEXT,
    zipcode TEXT,
    store_location TEXT,
    county_number TEXT,
    county TEXT,
    start_date DATE NOT NULL,
    end_date DATE,
    is_active BOOLEAN DEFAULT TRUE
);
-- Item Dimension (SCD Type 2)
CREATE TABLE Item_Dim (
    item_key INTEGER PRIMARY KEY AUTOINCREMENT,
    itemno TEXT NOT NULL,
    im_desc TEXT,
    category TEXT,
    category_name TEXT,
    pack FLOAT,
    bottle_volume_ml FLOAT,
    state_bottle_cost DECIMAL(10, 2),
    state_bottle_retail DECIMAL(10, 2),
    start_date DATE NOT NULL,
    end_date DATE,
    is_active BOOLEAN DEFAULT TRUE
);
-- Vendor Dimension (SCD Type 2)
CREATE TABLE Vendor_Dim (
    vendor_key INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_no TEXT NOT NULL,
    vendor_name TEXT,
    start_date DATE NOT NULL,
    end_date DATE,
    is_active BOOLEAN DEFAULT TRUE
);
-- Sales Fact Table
CREATE TABLE Sales_Fact (
    sales_key INTEGER PRIMARY KEY AUTOINCREMENT,
    invoice_line_no TEXT NOT NULL,
    store INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    store_key INTEGER NOT NULL,
    item_key INTEGER NOT NULL,
    vendor_key INTEGER NOT NULL,
    revenue DECIMAL(10, 2),
    profit DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    total_bottles_sold INTEGER,
    total_volume_sold_in_liters DECIMAL(10, 2),
    profit_margin DECIMAL(5, 2),
    average_bottle_price DECIMAL(10, 2),
    volume_per_bottle_sold DECIMAL(10, 2),
    processed_timestamp DATETIME,
    FOREIGN KEY (date_key) REFERENCES Date_Dim(date_key),
    FOREIGN KEY (store_key) REFERENCES Store_Dim(store_key),
    FOREIGN KEY (item_key) REFERENCES Item_Dim(item_key),
    FOREIGN KEY (vendor_key) REFERENCES Vendor_Dim(vendor_key),
    UNIQUE (invoice_line_no, store)
);
CREATE INDEX idx_date_key ON Sales_Fact(date_key);
CREATE INDEX idx_store_key ON Sales_Fact(store_key);
CREATE INDEX idx_item_key ON Sales_Fact(item_key);
CREATE INDEX idx_vendor_key ON Sales_Fact(vendor_key);