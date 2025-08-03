from google.cloud import bigquery

def load_to_bq(csv_file, dataset, table):
    client = bigquery.Client()
    table_id = f"{client.project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )
    with open(csv_file, "rb") as f:
        client.load_table_from_file(f, table_id, job_config=job_config).result()

if __name__ == "__main__":
    load_to_bq("processed/chunk_12.csv", "chisphung_liquor_dataset", "Staging_Sales")