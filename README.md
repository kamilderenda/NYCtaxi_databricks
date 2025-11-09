# NYCtaxi_databricks
This project shows how to build a simple data pipeline in Databricks using the Medallion Architecture.
The data comes from the NYC Taxi dataset, which includes information about taxi trips in New York City.
The goal is to load, clean, and prepare the data for analysis.

The project uses three layers:

00_Landing - stores downloaded data from source.

01_Bronze Layer – stores raw data (no changes).

02_Silver Layer – cleans and filters the data.

03_Gold Layer – prepares final tables for reports and dashboards.

04_export - export final tables to Data Lake Storage on Azure via Blob conncetion.

Each layer is saved as Delta tables in Databricks.

The pipeline is divided into three Databricks jobs:

Bronze Job – loads raw data.

Silver Job – cleans and transforms the data.

Gold Job – creates summary tables.

Jobs can be run manually or on a schedule.

Tools and Technologies:

Databricks
PySpark
Delta Lake

<img width="1215" height="342" alt="1" src="https://github.com/user-attachments/assets/257dbd78-a796-4e35-b389-adf202596718" />

<img width="1573" height="323" alt="Zrzut ekranu 2025-11-09 011119" src="https://github.com/user-attachments/assets/0ab68848-77e5-4a12-afdb-49ff884fc4d8" />

<img width="1577" height="280" alt="Zrzut ekranu 2025-11-09 180230" src="https://github.com/user-attachments/assets/86788e39-db50-4416-82fa-60dc750e4bed" />
<img width="1250" height="182" alt="Zrzut ekranu 2025-11-09 180134" src="https://github.com/user-attachments/assets/35d28bba-43a6-4642-99b9-fa77cf209d68" />
<img width="1337" height="630" alt="Zrzut ekranu 2025-11-09 180258" src="https://github.com/user-attachments/assets/e2c02aee-87f1-4a0f-a2f3-e4aecbadd974" />

