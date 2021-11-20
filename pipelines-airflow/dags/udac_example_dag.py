from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries, stmt_dict

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    
    'depends_on_past': False,
    'retries':3,
    'retries_delay':timedelta(minutes=5),
    'email_on_retry': False,
    'catchup':False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
finish_create_tables = DummyOperator(task_id='Finish_create_tables',  dag=dag)

# create tables
for sql_key, sql_stmt in stmt_dict.items():

    create_table = PostgresOperator(
        task_id=sql_key,
        dag=dag,
        postgres_conn_id="redshift",
        sql=sql_stmt
    )

    start_operator >> create_table
    create_table >> finish_create_tables
    

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    file_format='json',
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    file_format='json'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    load_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    load_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    load_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    load_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    load_query=SqlQueries.time_table_insert
)

songs_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks_songs',
    dag=dag,
    redshift_conn_id="redshift",
    table='songs'
)

users_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks_users',
    dag=dag,
    redshift_conn_id="redshift",
    table='users'
)

artists_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks_artists',
    dag=dag,
    redshift_conn_id="redshift",
    table='artists'
)

time_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks_time',
    dag=dag,
    redshift_conn_id="redshift",
    table='time'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

## Graph Dependencies

# "Start_operator", "create_table", and "finish_create_tables" are already defined
# at the begining of the script

finish_create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table 

load_song_dimension_table >> songs_quality_checks
load_user_dimension_table >> users_quality_checks
load_artist_dimension_table >> artists_quality_checks
load_time_dimension_table >> time_quality_checks

[songs_quality_checks, 
    users_quality_checks,
    artists_quality_checks,
    time_quality_checks] >> end_operator