from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


# DAG Definition
default_args = {
            'owner': 'udacity'
            ,'depends_on_past': False
            ,'start_date': datetime(2019, 1, 12)
            ,'retries': 3
            ,'retry_delay': timedelta(minutes=5)
            ,'catchup_by_default': False
            ,'email_on_retry': False
}
dag = DAG(  'udac_example_dag'
            ,default_args = default_args
            ,description = 'Load and transform data in Redshift with Airflow'
            ,schedule_interval = '0 * * * *'
            ,catchup = False
)


# start operator 
start_operator = DummyOperator(
    task_id = 'Begin_execution'
    ,dag = dag
)


# load bulk data from log_data and song_data
stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events'
    ,provide_context = False
    ,dag = dag
    ,table = "staging_events"
    ,s3_path = "s3://udacity-dend/log_data"
    ,redshift_connection = "redshift"
    ,amazon_conecction_user = "aws_credentials"
    ,region = "us-west-2"
    ,format_file = "s3://udacity-dend/log_json_path.json"
)
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs'
    ,provide_context = False
    ,dag = dag
    ,table = "staging_songs"
    ,s3_path = "s3://udacity-dend/song_data"
    ,redshift_connection = "redshift"
    ,amazon_conecction_user = "aws_credentials"
    ,region = "us-west-2"
    ,format_file = "auto"
)


# create fact table in redshift cluster
load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table'
    ,dag = dag
    ,redshift_connection = "redshift"
    ,table = "songplays"
    ,sql_query_variable = "songplay_table_insert"
    ,append_only = False
)

# create dimension tables in the redshift cluster
load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table'
    ,dag = dag
    ,redshift_connection = "redshift"
    ,table = "users"
    ,sql_query_variable = "user_table_insert"
    ,append_only = False
)
load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table'
    ,dag = dag
    ,redshift_connection = "redshift"
    ,table = "songs"
    ,sql_query_variable = "song_table_insert"
    ,append_only = False
)
load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table'
    ,dag = dag
    ,redshift_connection = "redshift"
    ,table = "artists"
    ,sql_query_variable = "artist_table_insert"
    ,append_only = False
)
load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table'
    ,dag = dag
    ,redshift_connection = "redshift"
    ,table = "time"
    ,sql_query_variable = "time_table_insert"
    ,append_only = False
)


# check data quality
run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks'
    ,dag = dag
    ,redshift_connection = "redshift"
    ,checks =   [
                    {'id_table': 'playid', 'table': 'songplays', 'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid IS NULL", 'expected_result': 0},
                    {'id_table': 'userid', 'table': 'users', 'check_sql': "SELECT COUNT(*) FROM users WHERE userid IS NULL", 'expected_result': 0},
                    {'id_table': 'songid', 'table': 'songs', 'check_sql': "SELECT COUNT(*) FROM songs WHERE songid IS NULL", 'expected_result': 0},
                    {'id_table': 'artistid', 'table': 'artists', 'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid IS NULL", 'expected_result': 0},
                    {'id_table': 'start_time', 'table': 'time', 'check_sql': "SELECT COUNT(*) FROM time WHERE start_time IS NULL", 'expected_result': 0}
                ]
)


end_operator = DummyOperator(task_id = 'End_execution', dag = dag)


start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator