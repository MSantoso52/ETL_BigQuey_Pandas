# ETL_BigQuey_Pandas
Building an ETL Pipeline for CO2 Emission Data Analysis Using BigQuery and Python

# *Overview*
To design and implement ETL (Extract, Transform, Load) pipelines to ingest data from external sources, process it, and store it in a data warehouse for analysis. This simulates a real-world data engineering workflow, where you handle data ingestion, ensure data quality, and enable downstream analytics. The project uses Python for scripting and the google-cloud-bigquery library to interact with BigQuery.
* Extracts data from external source (CSV file)
* Transforms the data (e.g., cleaning, aggregating, converting units).
* Loads the processed data into Google BigQuery for storage and querying.

# *Prerequisites*
To follow along this project need to available on system:
* Install google cloud CLI
  ```bash
  # Install dependency
  sudo apt update
  sudo apt install apt-transport-https ca-certificates gnupg curl

  #  Import the Google Cloud Public Key
  curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg

  # Initialize the gcloud CLI
  gcloud init
  ```
  This command will:
  * Launch a browser window for you to log in with your Google Cloud account.
  * Set up your default project.
  * Configure your default compute region/zone (if applicable).
  You are now ready to use the gcloud command-line tool.
  
* Python library to interact with Google Cloud
  ```bash
  pip install google-cloud-bigquery db-dtypes
  ```

# *Project Flow*
1. Extract CSV file into pandas
   ```python
   try:
        data = pd.read_csv(file_path)
        print(f"Successfully extract '{file_path}' into Data Frame")
        return data

    except Exception as e:
        print(f"Unexpected Error occured: {e}")
        return None
   ```
2. Transform, data cleansing using pandas
   ```python
    # Rename columns for consistency
    raw.columns = ['location', 'percent_global_total', 'fossil_emissions_2023', 'fossil_emissions_2000', 'percent_change_from_2000']

    # Clean 'percent_global_total': remove '%' and convert to float
    raw['percent_global_total'] = raw['percent_global_total'].str.replace('%', '', regex=False).astype(float)

    # Clean fossil emissions columns: remove commas and convert to float
    raw['fossil_emissions_2023'] = raw['fossil_emissions_2023'].str.replace(',', '', regex=False).astype(float)
    raw['fossil_emissions_2000'] = pd.to_numeric(raw['fossil_emissions_2000'].str.replace(',', '', regex=False), errors='coerce')
   ```
3. Load data into BigQuery table
   * Define schema for BigQuery table
     ```python
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
     ```
   * Create table if doesn't exists
     ```python
      # Create table if it doesn't exist
      try:
        client.get_table(table_ref)
      except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Created table {table_ref}")
     ```
   * Insert transformed data into rows
     ```python
     # Load DataFrame to BigQuery
      job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_APPEND')
      job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
      job.result()  # Wait for job to complete
      print(f"Loaded {len(df)} rows to {table_ref}")
     ```
5. Query & Veriry
   * Run simple SQL query on BigQuery Studio
     ```python
     client = bigquery.Client(project=project_id)
     query = f"""
     SELECT * FROM `{project_id}.{dataset_id}.{table_id}`
     LIMIT 10
     """
     results = client.query(query).to_dataframe()
     print("Latest 5 records:")
     print(results)
     ```
   * Ensure the data integrity 
