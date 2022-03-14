from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):
    #template_fields = ("s3_key",)
    ui_color = '#008080'
    # Last brace accepts additional parameters to handle file types. ex: for CSV -> IGNOREHEADER True DELIMITER ';' CSV
    copy_csv_redshift = """
        COPY {}
        FROM '{}'
        IAM_ROLE '{}'
        REGION '{}'
        {}
    """
    copy_parq_redshift = """
        COPY {}
        FROM '{}'
        IAM_ROLE '{}'
        {}
    """    

        #FORMAT AS PARQUET
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_iam_arn="",
                 aws_region="us-east-1",
                 table_name="",
                 file_type='CSV',
                 s3_bucket="",
                 s3_key="",
                 extra_params="",
                 *args, **kwargs):

        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.aws_iam_arn = aws_iam_arn
        self.aws_region = aws_region
        self.table_name = table_name
        self.file_type = file_type
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.extra_params = extra_params
        

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        

        self.log.info(f"Clearing data from destination Redshift table {self.table_name}")
        redshift.run("TRUNCATE {}".format(self.table_name))

        self.log.info(f"Copying data from s3://{self.s3_bucket}/{self.s3_key} to Redshift {self.table_name}")
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        # extra_params configured for various file types and format conditions
        if self.file_type=='CSV':
            formatted_sql = S3ToRedshiftOperator.copy_csv_redshift.format(
                self.table_name,
                s3_path,
                self.aws_iam_arn,
                self.aws_region,
                self.extra_params
                )
        elif self.file_type=='PARQUET':
            formatted_sql = S3ToRedshiftOperator.copy_parq_redshift.format(
                self.table_name,
                s3_path,
                self.aws_iam_arn,
                self.extra_params
                )
        else:
            self.log.info(f"unsupported file type.")

        redshift.run(formatted_sql)

        self.log.info(f"Loading complete!")