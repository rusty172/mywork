from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 6, 27),
    'depends_on_past' : False,
    'retries' : 0,
    'retry_delay' : timedelta(minutes=5),
    'email_on_retry' : False,
}

dag = DAG('data_pipeline_ETL',
          default_args=default_args,
          description='Load & Transform Data To Redshift with Airflow',
          schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_event_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key= "log_data",    
    s3_param = "s3://udacity-dend/log_json_path.json", 
    aws_credentials_id="aws_credentials",
    data_format = "json",
    provide_context=True,
    region = "us-west-2"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key = 'song_data/A/A/A/',
    s3_param="auto",
    aws_credentials_id="aws_credentials",
    data_format = "json",
    region = "us-west-2"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag = dag,
    postgres_conn_id="redshift",
    table= "songplays",    
    sql = SqlQueries.songplay_table_insert    
)

load_user_dim_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    postgres_conn_id="redshift",
    table="users1",
    sql= SqlQueries.user_table_insert
)

load_song_dim_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    postgres_conn_id="redshift",
    table="songs",
    sql= SqlQueries.song_table_insert
)

load_artist_dim_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    postgres_conn_id="redshift",
    table="artists",
    sql= SqlQueries.artist_table_insert,
    append_insert=True,
    primary_key="artistid"
)

load_time_dim_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    postgres_conn_id="redshift",
    table="time",
    sql= SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    postgres_conn_id='redshift',
    query='select count(*) from songs where songid is null;',
    result=0   
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator  >> stage_events_to_redshift >> load_songplays_table
start_operator  >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dim_table >> run_quality_checks
load_songplays_table >> load_song_dim_table >> run_quality_checks
load_songplays_table >> load_artist_dim_table >> run_quality_checks
load_songplays_table >> load_time_dim_table >> run_quality_checks
run_quality_checks >> end_operator
