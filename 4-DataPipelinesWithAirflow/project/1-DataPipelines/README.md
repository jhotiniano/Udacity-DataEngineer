## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more 
automation and monitoring to their data warehouse ETL pipelines and come to the 
conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade 
data pipelines that are dynamic and built from reusable tasks, can be monitored, and 
allow easy backfills. They have also noted that the data quality plays a big part 
when analyses are executed on top the data warehouse and want to run tests against 
their datasets after the ETL steps have been executed to catch any discrepancies in 
the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse 
in Amazon Redshift. The source datasets consist of CSV logs that tell about user 
activity in the application and JSON metadata about the songs the users listen to.


## ELT Process

The schema of the ELT is this DAG:

![DAG](images/dag.png)


## Sources

My personl bucket contains all data:

Log data: s3://udacity-project5-jop/log_data <br>
Song data: s3://udacity-project5-jop/song_data


## Destinations

Data is inserted into Amazon Redshift Cluster.

2 staging tables were added: Stage_events, Stage_songs

Star schema:
* Fact Table: songplays 
* Dimension Tables: users, songs, artists, time


#### Pre requisites

Tables must be created in Redshift before executing the DAG workflow. <br>
The create tables script can be found in: create_tables.sql


## Structure

*create_tables.sql: Contains the DDL for all tables <br>
*dags/udac_example_dag.py: The DAG configuration file <br>
*plugins/operators/stage_redshift.py: Operator to read files from S3 and load into redshift staging tables <br>
*plugins/operators/load_fact.py: Operator to load the fact table in redshift <br>
*plugins/operators/load_dimension.py: Operator to load the dime table in redshift <br>
*plugins/operators/data_quality.py: Operator for data quality checking <br>
*plugins/helpers/sql_queries: Redshift statements used in the DAG


## Data Quality Checks

Data quality checking performe to count the total records each table. 