# ETL Iowa Liquor Google Cloud Project

This repository is a variant of [ETL Iowa Liquor Sale](https://github.com/dangnha/ETL_Iowa_liquor_sale).
I have modified the original project to use Google Cloud services, including Google Cloud Storage (GCS) for data storage and BigQuery for data processing and analysis. I also modularized the code to enhance maintainability and scalability. And included docker to make it easier to run the project in a containerized environment.

## Prerequisites
- Python 3.12 or later
- Google Cloud SDK installed and configured
- Docker installed 


## Step-by-Step Setup and Execution

Follow these steps to set up and run the ETL pipeline:

1. **Clone the Repository** 

   - Clone the repository to your local machine and install dependencies:
     ```bash
     git clone https://github.com/chisphung/ETL_Liquor_GCS.git
     cd ETL_Liquor_GCS
     pip install -r requirements.txt
     ```
2. **Set Up Google Cloud Project** 
    - Create a new Google Cloud project or use an existing one.
    - Enable the BigQuery and Google Cloud Storage APIs:
      ```bash
      gcloud services enable bigquery.googleapis.com
      gcloud services enable storage.googleapis.com
      ```
    - Authenticate your Google Cloud SDK:
      ```bash
      gcloud auth login
      ```
    - Set your project ID:
      ```bash
      gcloud config set project your-project-id
      ```
3. **Set Up Bigquery**
    - Create a BigQuery dataset named `liquor_sales`:
      ```bash
      bq mk liquor_sales
      ```
    - Create the necessary tables in BigQuery using the SQL scripts provided in the `sql/` folder.
4. **Set Up Google Cloud Storage (GCS)**
   - Create a GCS bucket to store input and output data:
     ```bash
     gsutil mb gs://your-bucket-name
     ```
5. **Set Up Environment Variables**
    - In `config.py`, set the environment variables as the example.
    - Make sure that you have to change the variable names to match your GCS bucket and BigQuery dataset.

6. **Simulate Data** 

   - Run the Jupyter notebook `Simulate_data.ipynb` to generate sample data (100k and 1M rows):
     ```bash
     jupyter notebook Simulate_data.ipynb
     ```
     - Execute all cells to generate CSV files (e.g., `sales_data_100k.csv`, `sales_data_1M.csv`).
     - Move the CSV files to the `input/` folder:
       ```bash
       mv sales_data_*.csv input/
       ```

7. **Run the ETL Process** 

   - Run the ETL script to process data and load it into `Sales_Fact`:
     ```bash
     python src/main.py
     ```
     - The script uses chunked processing to manage memory and handle issues like duplicate keys, merging sales data with dimension tables.

8. **Monitor Processing** 

   - Check the console output for processing status and any errors.
   - You can also monitor the progress in the Google Cloud Console under BigQuery and GCS.
   - Looker is also a great tool to visualize the data in BigQuery.

9. **Docker is provided**
   - To run the ETL process in a Docker container, build the Docker image:
     ```bash
     docker build -t etl-liquor-gcs .
     ```
   - Run the Docker container:
     ```bash
     docker run --rm -v $(pwd)/input:/app/input -v $(pwd)/output:/app/output etl-liquor-gcs
     ```
   - Note that you can also push the Docker image to a container registry (e.g., Google Container Registry) for deployment in a cloud environment. This allows for using Cloud Run or Kubernetes for scalable execution.
   - After deploying on Cloud Run or Kubernetes, you can trigger the ETL process using HTTP requests or scheduled jobs.
   - Cloud Workflows can also be used to orchestrate the ETL process, allowing for more complex workflows and scheduling and writing logs to Cloud Logging.

## Troubleshooting

- **Duplicate Keys in Dimension Tables** 

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

- **Memory Issues** 

  - The ETL script processes data in 1,000-row chunks. For large datasets (e.g., 1M rows), ensure sufficient memory or adjust `chunk_size` in `src/load.py`.

- **Unmatched Keys** 

  - If rows are dropped due to missing keys, log unmatched values:
    ```sql
    SELECT DISTINCT date FROM sales_data WHERE date NOT IN (SELECT date FROM Date_Dim);
    ```
  - Update dimension tables to include missing keys.

## Conclusion
This project demonstrates a complete ETL pipeline using Google Cloud services, modularized for maintainability. The use of Docker allows for easy deployment and execution in various environments. The modular design enables future enhancements, such as adding more data sources or processing steps.
