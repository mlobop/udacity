from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from queries import SqlQueries

# Defines default arguments based on the project requisites
default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag('mlp-dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    #
    # Operators
    #

    # Initial operator: starts the execution
    start_operator = DummyOperator(
        task_id='Begin_execution',
        dag=dag
    )

    # Task to copy data from S3 into Redshift's events table
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag,
        conn_id='redshift',
        aws_credentials='aws_credentials',
        target_table='staging_events',
        s3_bucket='udacity-dend',
        s3_subfolder="log_data",
        region='us-west-2',
        json_option='s3://udacity-dend/log_json_path.json'
    )

    # Task to copy data from S3 into Redshift's songs table
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag=dag,
        conn_id='redshift',
        aws_credentials='aws_credentials',
        target_table='staging_songs',
        s3_bucket='udacity-dend',
        s3_subfolder="song_data",
        region='us-west-2',
        json_option='auto'
    )

    # Task to insert data into facts table
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        conn_id = 'redshift',
        target_table = 'songplays',
        select_query = SqlQueries.songplay_table_insert
    )

    # Task to insert data into user dimension table
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        conn_id = 'redshift',
        target_table = 'users',
        select_query = SqlQueries.user_table_insert,
        truncate_or_append = True
    )

    # Task to insert data into song dimension table
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        conn_id = 'redshift',
        target_table = 'songs',
        select_query = SqlQueries.song_table_insert,
        truncate_or_append = True
    )

    # Task to insert data into artist dimension table
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        conn_id = 'redshift',
        target_table = 'artists',
        select_query = SqlQueries.artist_table_insert,
        truncate_or_append = True
    )

    # Task to insert data into time dimension table
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        conn_id = 'redshift',
        target_table = 'time',
        select_query = SqlQueries.time_table_insert,
        truncate_or_append = True
    )

    # Task to perform quality checks
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        dag=dag,
        conn_id = 'redshift',
        quality_checks = [
            {'test_sql': 'SELECT COUNT(*) FROM songplays WHERE songplay_id IS NULL;', 'expected_result': 0, 'tab_name': 'songplays'},
            {'test_sql': 'SELECT COUNT(*) FROM users WHERE user_id IS NULL;', 'expected_result': 0, 'tab_name': 'users'},
            {'test_sql': 'SELECT COUNT(*) FROM songs WHERE song_id IS NULL;', 'expected_result': 0, 'tab_name': 'songs'},
            {'test_sql': 'SELECT COUNT(*) FROM artists WHERE artist_id IS NULL;', 'expected_result': 0, 'tab_name': 'artists'},
            {'test_sql': 'SELECT COUNT(*) FROM time WHERE start_time IS NULL;', 'expected_result': 0, 'tab_name': 'time'},
        ]
    )

    # Final operator: finishes the execution
    end_operator = DummyOperator(
        task_id='End_execution',
        dag=dag
    )

    #
    # Task ordering for the DAG tasks
    #

    # Data is copied into staging events and songs tables after the start of the execution 
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    # Data is loaded from staging tables into fact table
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    # Data is inserted afterwards into all the dimension tables
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    # Data quality check is performed
    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    # Finished the execution
    run_quality_checks >> end_operator
    
final_project_dag = final_project()