import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import os

# Configuration
PROJECT_ID = 'psychic-mason-473812-u3'  # Your Google Cloud Project ID
DATASET_ID = 'co2_emission'  # BigQuery dataset name
TABLE_ID = 'fossil'  # BigQuery table name
FILE_PATH = './CO2 Emission Country.csv' # File path for data

# Step 1: Extract data from file
def extract_data(file_path: str)-> pd.DataFrame | None:
    
    # Checking file availability
    if not os.path.exists(file_path):
        print(f"Error: File Not Found at path: {file_path}")
        return None
    
    try:
        data = pd.read_csv(file_path)
        print(f"Successfully extract '{file_path}' into Data Frame")
        return data

    except Exception as e:
        print(f"Unexpected Error occured: {e}")
        return None

# Step 2: Transform the data
def transform_data(raw) -> pd.DataFrame:
    # Rename columns for consistency
    raw.columns = ['location', 'percent_global_total', 'fossil_emissions_2023', 'fossil_emissions_2000', 'percent_change_from_2000']

    # Clean 'percent_global_total': remove '%' and convert to float
    raw['percent_global_total'] = raw['percent_global_total'].str.replace('%', '', regex=False).astype(float)

    # Clean fossil emissions columns: remove commas and convert to float
    raw['fossil_emissions_2023'] = raw['fossil_emissions_2023'].str.replace(',', '', regex=False).astype(float)
    raw['fossil_emissions_2000'] = pd.to_numeric(raw['fossil_emissions_2000'].str.replace(',', '', regex=False), errors='coerce')

    # Clean 'percent_change_from_2000': handle textual anomalies
    raw['percent_change_from_2000'] = raw['percent_change_from_2000'].str.replace('"', '', regex=False)  # Remove any extra quotes
    raw['percent_change_from_2000'] = raw['percent_change_from_2000'].str.replace(',', '', regex=False)  # Remove commas
    raw['percent_change_from_2000'] = raw['percent_change_from_2000'].str.replace('no change', '0', regex=False)
    raw['percent_change_from_2000'] = raw['percent_change_from_2000'].str.replace('+', '', regex=False)
    raw['percent_change_from_2000'] = raw['percent_change_from_2000'].str.replace('%', '', regex=False)
    raw['percent_change_from_2000'] = raw['percent_change_from_2000'].str.replace('\u2212', '-', regex=False)  # Unicode minus to normal minus

    raw['percent_change_from_2000'] = pd.to_numeric(raw['percent_change_from_2000'], errors='coerce')

    # Optional: print cleaned data for verification
    return raw

# Step 3: Load data into BigQuery
def load_to_bigquery(df, project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    
    # Full table reference
    table_ref = f'{project_id}.{dataset_id}.{table_id}'
    
    # Define schema
    schema = [
        bigquery.SchemaField('location', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('percent_global_total', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('fossil_emissions_2023', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('fossil_emissions_2000', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('percent_change_from_2000', 'FLOAT', mode='NULLABLE'),
    ]
    
    # Create table if it doesn't exist
    try:
        client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Created table {table_ref}")
    
    # Load DataFrame to BigQuery
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_APPEND')
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for job to complete
    print(f"Loaded {len(df)} rows to {table_ref}")

# Step 4: Query to verify
def query_bigquery(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT * FROM `{project_id}.{dataset_id}.{table_id}`
    LIMIT 10
    """
    results = client.query(query).to_dataframe()
    print("Latest 5 records:")
    print(results)

# Main ETL function
def run_etl():
    raw_data = extract_data(FILE_PATH)
    transformed_df = transform_data(raw_data)
    load_to_bigquery(transformed_df, PROJECT_ID, DATASET_ID, TABLE_ID)
    query_bigquery(PROJECT_ID, DATASET_ID, TABLE_ID)

if __name__ == '__main__':
    run_etl()
