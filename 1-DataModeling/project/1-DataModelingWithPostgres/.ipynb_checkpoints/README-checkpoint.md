# Introduccion

Sparkfy, is a music company in streaming. Seeks to analyze the information on: users, user activity and song detail.
The data is stored in a JSON format.

The Analytics team is interested in understanding that the users listen. So the data engineer must design a database (postgres) with tables for analysis consultations.

The objective is to import the data (JSON format) on an optimized data model. Therefore, we will use the star scheme. The data model will live on the database of postgres.

The star scheme is characterized by concentrating one or more tables that reference to other dimensional tables. Sparkfy could take advantage of the star scheme to better understand how his area / business works. Example:

Sparkify would use the following star scheme:
* Table of Acts: Songplays: Attributes References to dimension tables.
* Tables of dimensions: Users, songs, artists and time table.

The existing databases will help the Analytics team apply different types of analysis to recommend a Sparkify user.
* Favorite user songs based on the day of the week
* Help recommend the most popular songs of the day / week.


# ETL Pipeline
* Create FACT table from the dimensison tables and log_data called songplays.
* Create DIMENSION songs, artist from extracting songs_data by selected columns.
* Create DIMENSION users, time from extracting log_data by selected columns.


# Details
1. test.ipynb, Displays the content for each table in the database.
2. create_tables.py, Remove and create star schema tables. Can be executed to reset the star schema.
3. etl.ipynb, Read and process data (songs and logs) and load to the star schema. The notebook contains detailed instructions of the ETL process for each table.
4. etl.py, Lee and process data (songs and records) and load to the star schema. 
5. sql_queries.py, Contains SQL queries.


# Execution
$ python3 create_tables.py
$ python3 etl.py


# Notebooks
etl.ipynb
test.ipynb