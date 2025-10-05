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
