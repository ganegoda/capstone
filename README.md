# Udacity Data Engineering Nanodegree Capstone Project
## Table of Contents

- [Introduction](#introduction)

- [Data Model](#data-model)

- [Airflow Data Pipeline](#airflow-data-pipeline)

- [Initial Setup](#initial-setup)

- [How to Use](#how-to-use)


## Introduction
For my data engineering capstone project I developed a data pipeline that creates an analytical database and supporting tables. Analytical database contains US immigration data populated on a monthly basis. Additional datasets are also available in staging tables. Insights can be drawn from main analytical tables or combining with other information tables provided. All tables are hosted in Amazon Redshift Database and ETL/ELT pipeline was developed using Apache Airflow.

### Datasets
Following datasets were used to create analytical database:
- I94 Immigration Data: This dataset comes from the US National Tourism and Trade Office. Each data file contains monthly information on international visitors arrival. Data fields include information on arrival departure time frame, citizenship country, residence country, arrival mode, and some traveller information such as birth year, age at arrival, occupation, gender etc. Each file contains 28 data columns and 3 million rows. Immigration data comes with a data dictionary that defines column contents of the main dataset which can be parsed and used in building the data model.


*I94 immigration data sample:*

![I94 immigration data sample1](./images/immig1.png)
![I94 immigration data sample2](./images/immig2.png)


- World Temperature Data: This kaggle dataset contains city, country, latitude, longitude, average temperature, and temperature uncertainty data.

 *World temperature data sample:*

![world-temperature](./images/world-temp.png)

- U.S. City Demographic Data: This dataset contains information about the demographics of all US cities and census-designated places with a population greater than or equal to 65,0000. Dataset comes from OpenSoft.

*U.S. city demographic data sample:*

![city-demo](./images/city-demo.png)

- Airport Codes: This dataset contains data on airport codes and corresponding cities. According to wikipedia, The airport codes may refer to either IATA airport code, a three-letter code which is used in passenger reservation, ticketing and baggage-handling systems, or the ICAO airport code which is a four letter code used by ATC systems and for airports that do not have an IATA airport code. 

*Airport Codes data sample:*

![Airport-codes](./images/airport.png)

- Manually Collected Data: Additional datasets describing gender definitions, and visa classes were collected through online research. These datasets improves the capabilities of main analytical database.


## Data Model
U.S. Immigration analytical database has a star schema with one fact table and multiple dimension tables. Dimension tables are directly populated through truncate, copy pattern or special operator that parses and loads the data. Dimension tables are relatively small in size. Consequently, truncate-copy pattern is a reasonable way to to maintain idempotent data pipeline without compromising the performance. Dimension tables are distributed across all nodes for faster query performance. Immigration dataset is first loaded into staging a staging table, which will then be cleaned and populates that fact table.

Additional datasets containing world temperature, US city demographics, and airport codes can be combined with main database to answer various analytical questions. These datasets are loaded into staging tables, cleaned and transformed into more analytical friendly formats.

Database schema is shown below:

![Main-dataset](./images/er_capstone.png)

## Airflow Data Pipeline
Custom operators were developed for loading datasets from s3 bucket to RedShift, cleaning, and validation. 
- SASfileToRedshiftOperator : Parse and load tables from SAS data dictionary to Redshift.
- S3ToRedshiftOperator : Load files in CSV or Parquet format to Redshift using copy command.
- LoadFactOperator : Clean and load immigration fact table from staging table.
- CleanTablesOperator : Perform various data cleanup operations, for example fill null values, convert to numeric format etc.
- DataQualityOperator : performs data quality checks to ensure tables are populated without errors using pre-defined set of queries and expected result.

Airflow data pipeline is shown below:

![pipeline](./images/graph_success.png)




## Initial Setup



## How to Use
