# ETL Iowa Liquor Sales 🍶

This repository contains an **ETL (Extract, Transform, Load)** pipeline for processing Iowa liquor sales data into a **SQLite** database. The pipeline transforms raw sales data into a star schema with dimension and fact tables, efficiently handling large datasets (100k and 1M rows) using chunked processing.

## 📋 Prerequisites

- 🐍 **Python 3.8+**: For running ETL scripts and simulating data.
- 💻 **Visual Studio Code (VS Code)**: For development and SQLite integration.
- 🔌 **VS Code Extension**: SQLite Explorer or SQLite Viewer for database interaction.
- 📦 **Python Libraries**: Install via `requirements.txt`:
  ```bash
  pip install -r requirements.txt
  ```

## 🚀 Step-by-Step Setup and Execution

Follow these steps to set up and run the ETL pipeline:

1. **Clone the Repository** 🗂️

   - Clone the repository to your local machine and install dependencies:
     ```bash
     git clone https://github.com/dangnha/ETL_Iowa_liquor_sale.git
     cd ETL_Iowa_liquor_sale
     pip install -r requirements.txt
     ```

2. **Set Up SQLite in VS Code** 🛠️

   - Install the **SQLite Explorer** or **SQLite Viewer** extension:
     - Open VS Code.
     - Go to Extensions (`Ctrl+Shift+X` or `Cmd+Shift+X` on macOS).
     - Search for "SQLite Explorer" or "SQLite Viewer" and install it.

3. **Create the SQLite Database** 🗄️

   - Create a new SQLite database named `liquor_sales.db`:
     ```bash
     sqlite3 liquor_sales.db
     ```
     - Exit the SQLite CLI by typing `.exit`.

4. **Connect to the SQLite Database in VS Code** 🔗

   - Open VS Code and the cloned repository folder.
   - Use the SQLite Explorer extension:
     - Press `Ctrl+P` (or `Cmd+P` on macOS), type `> SQLite: Open Database`, and select `liquor_sales.db`.
     - The database will appear in the SQLite Explorer panel.

5. **Create Database Tables** 📑

   - Run the SQL scripts to create dimension and fact tables:
     - Locate the SQL scripts in the `sql/` folder (e.g., `create_tables.sql`).
     - Run the scripts using SQLite Explorer or in the SQLite CLI:
       ```bash
       sqlite3 liquor_sales.db < sql/create_tables.sql
       ```

6. **Simulate Data** 📊

   - Run the Jupyter notebook `Simulate_data.ipynb` to generate sample data (100k and 1M rows):
     ```bash
     jupyter notebook Simulate_data.ipynb
     ```
     - Execute all cells to generate CSV files (e.g., `sales_data_100k.csv`, `sales_data_1M.csv`).
     - Move the CSV files to the `input/` folder:
       ```bash
       mv sales_data_*.csv input/
       ```

7. **Run the ETL Process** ⚙️

   - Run the ETL script to process data and load it into `Sales_Fact`:
     ```bash
     python src/etl_process.py
     ```
     - The script uses chunked processing to manage memory and handle issues like duplicate keys, merging sales data with dimension tables.

8. **Monitor Processing** 📈

   - Watch the ETL script’s logs (e.g., "Processing chunk 34 (1000 rows)...") for progress and errors.
   - If errors like "Duplicate dates detected" occur, check dimension tables for duplicates (see Troubleshooting).

9. **View Tables in SQLite** 👀
   - Open the database in VS Code:
     - Press `Ctrl+P`, type `> SQLite: Open Database`, and select `liquor_sales.db`.
     - Use SQLite Explorer to view tables (`Date_Dim`, `Store_Dim`, `Item_Dim`, `Vendor_Dim`, `Sales_Fact`).
     - Run queries to verify data:
       ```sql
       SELECT * FROM Sales_Fact LIMIT 10;
       SELECT COUNT(*) FROM Sales_Fact;
       ```

## 🛑 Troubleshooting

- **Duplicate Keys in Dimension Tables** 🔑

  - If the ETL script fails with "Duplicate dates detected," check for duplicates:
    ```sql
    SELECT date, COUNT(*) FROM Date_Dim GROUP BY date HAVING COUNT(*) > 1;
    SELECT store_id, COUNT(*) FROM Store_Dim GROUP BY store_id HAVING COUNT(*) > 1;
    SELECT itemno, COUNT(*) FROM Item_Dim GROUP BY itemno HAVING COUNT(*) > 1;
    SELECT vendor_no, COUNT(*) FROM Vendor_Dim GROUP BY vendor_no HAVING COUNT(*) > 1;
    ```
  - Deduplicate using:
    ```sql
    DELETE FROM Date_Dim WHERE rowid NOT IN (
        SELECT MIN(rowid) FROM Date_Dim GROUP BY date
    );
    -- Repeat for Store_Dim, Item_Dim, Vendor_Dim
    ```
  - Ensure `UNIQUE` constraints are defined in `sql/create_tables.sql`.

- **Memory Issues** 💾

  - The ETL script processes data in 1,000-row chunks. For large datasets (e.g., 1M rows), ensure sufficient memory or adjust `chunk_size` in `src/etl_process.py`.

- **Unmatched Keys** ⚠️

  - If rows are dropped due to missing keys, log unmatched values:
    ```sql
    SELECT DISTINCT date FROM sales_data WHERE date NOT IN (SELECT date FROM Date_Dim);
    ```
  - Update dimension tables to include missing keys.

- **Database Connection Issues** 🔌
  - Ensure `liquor_sales.db` is in the project root and accessible by VS Code and the ETL script.

## ℹ️ Notes

- 📁 The ETL script expects input CSV files in the `input/` folder.
- 📊 The pipeline is optimized for large datasets (100k and 1M rows) using chunked processing.
- 🔍 Dimension tables must be populated with unique keys before running the ETL script, as they provide foreign keys (`date_key`, `store_key`, `item_key`, `vendor_key`) for `Sales_Fact`.

## 🤝 Contributing

Contributions are welcome! Submit a pull request or open an issue on the [GitHub repository](https://github.com/dangnha/ETL_Iowa_liquor_sale).

## 📜 License

© 2025 dangnha. All rights reserved.
