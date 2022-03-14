from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.sas_to_redshift import SASfileToRedshiftOperator
from operators.copy_to_redshift import S3ToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.clean_tables import CleanTablesOperator
from operators.data_quality import DataQualityOperator
#from airflow.plugins.operators.sas_to_redshift import SASfileToRedshiftOperator
from helpers.tables import Tables
from helpers.sql_queries import SqlQueries

#from helpers import SqlQueries


from airflow.models import Variable

# Config
#s3_bucket_name = Variable.get("s3_bucket")
aws_iam_arn = Variable.get("aws_iam_arn")

default_args = {
    'owner': 'hg',
    'start_date': datetime(2022, 2, 28),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
    'email_on_retry' : False,
}


dag = DAG('elt_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='0 * * * *'
          schedule_interval='@monthly',

          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator = DummyOperator(task_id='End_execution',  dag=dag)

# Copy tables from SAS file
for table in Tables.sas_tables:
    load_sas_table = SASfileToRedshiftOperator(
    task_id='loading_table_{}_from_sas_file'.format(table.get('table_name')),
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="hg-dend", 
    s3_key="I94_SAS_Labels_Descriptions.SAS", 
    aws_region="us-east-1",
    parse_value=table.get('parse_string'),
    end_string=table.get('end_string'),
    table_name=table.get('table_name'),
    table_columns=table.get('table_columns'),
    )

    # Perform quality checks
    run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks_on_{}'.format(table.get('table_name')),
    dag=dag,
    redshift_conn_id="redshift",
    table_name=table.get('table_name'),
    dq_test_list=table.get('dq_checks')
    )

    start_operator>>load_sas_table>>run_quality_checks>>end_operator
    #start_operator>>run_quality_checks>>end_operator


# Copy tables from csv and parquet
for table in Tables.csv_parq_tables:
    load_from_csv_parq = S3ToRedshiftOperator(
    task_id='loading_table_{}'.format(table.get('table_name')),
    dag=dag,
    redshift_conn_id="redshift",
    aws_iam_arn=aws_iam_arn,
    aws_region="us-east-1",
    table_name=table.get('table_name'),
    file_type=table.get('file_type'),
    s3_bucket="hg-dend", 
    s3_key=table.get('key'), 
    extra_params=table.get('extra_params')
    )


    # Perform quality checks
    run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks_on_{}'.format(table.get('table_name')),
    dag=dag,
    redshift_conn_id="redshift",
    table_name=table.get('table_name'),
    dq_test_list=table.get('dq_checks')

    )
    start_operator>>load_from_csv_parq>>run_quality_checks>>end_operator
    #start_operator>>run_quality_checks>>end_operator

# Copy tables staging table airport code, world temp, us city demo

for table in Tables.csv_stg_tables:
    load_clean_from_csv_parq = S3ToRedshiftOperator(
    task_id='loading_table_{}'.format(table.get('table_name')),
    dag=dag,
    redshift_conn_id="redshift",
    aws_iam_arn=aws_iam_arn,
    aws_region="us-east-1",
    table_name=table.get('table_name'),
    file_type=table.get('file_type'),
    s3_bucket="hg-dend", 
    s3_key=table.get('key'), 
    extra_params=table.get('extra_params')
    )

#     # Clean data
    clean_stg_table = CleanTablesOperator(
    task_id='clean_table_{}'.format(table.get('table_name')),
    dag=dag,
    redshift_conn_id="redshift",
    table=table.get('table_name'),
    sql_stmt= SqlQueries.clean_sql.get(table.get('table_name'))

    )
    
    
    # Perform quality checks
    run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks_on_{}'.format(table.get('table_name')),
    dag=dag,
    redshift_conn_id="redshift",
    table_name=table.get('table_name'),
    dq_test_list=table.get('dq_checks')

    )
    
    start_operator>>load_clean_from_csv_parq>>clean_stg_table>>run_quality_checks>>end_operator
    #start_operator>>clean_stg_table>>run_quality_checks>>end_operator



    


load_immigraion_stg_table = S3ToRedshiftOperator(
    task_id='loading_table_{}'.format(Tables.parq_immigration.get('table_name')),
    dag=dag,
    redshift_conn_id="redshift",
    aws_iam_arn=aws_iam_arn,
    aws_region="us-east-1",
    table_name=Tables.parq_immigration.get('table_name'),
    file_type=Tables.parq_immigration.get('file_type'),
    s3_bucket="hg-dend", 
    s3_key=Tables.parq_immigration.get('key'), 
    extra_params=Tables.parq_immigration.get('extra_params')
    )


# clean data and load immigration fact table from immigration staging table
load_clean_immigration_table = LoadFactOperator(
    task_id='load_public.immigration_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="public.immigration",
    sql_stmt=SqlQueries.clean_load_immigration
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks_on_{}'.format(Tables.parq_immigration.get('table_name')),
    dag=dag,
    redshift_conn_id="redshift",
    table_name=Tables.parq_immigration.get('table_name'),
    dq_test_list=Tables.parq_immigration.get('dq_checks')

    )

start_operator>>load_immigraion_stg_table>>load_clean_immigration_table>>run_quality_checks>>end_operator
#start_operator>>load_clean_immigration_table>>run_quality_checks>>end_operator








# end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# start_operator >> stage_events_to_redshift
# start_operator >> stage_songs_to_redshift

# stage_events_to_redshift >> load_songplays_table
# stage_songs_to_redshift >> load_songplays_table

# load_songplays_table >> load_user_dimension_table
# load_songplays_table >> load_song_dimension_table
# load_songplays_table >> load_artist_dimension_table
# load_songplays_table >> load_time_dimension_table

# load_user_dimension_table >> run_quality_checks
# load_song_dimension_table >> run_quality_checks
# load_artist_dimension_table >> run_quality_checks
# load_time_dimension_table >> run_quality_checks

# run_quality_checks >> end_operator
