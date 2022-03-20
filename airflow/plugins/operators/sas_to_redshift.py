from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from sqlalchemy import create_engine, text


class SASfileToRedshiftOperator(BaseOperator):
    """ Custom operator parsing SAS file and loading to tables.
    Attributes:
        ui_color (str): color code for task in airflow UI
        truncate_template (str): templated turncate sql statement
    """
    ui_color = '#358140'

    truncate_template = "TRUNCATE {};"

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 aws_region="us-east-1",
                 parse_value="",
                 end_string="",
                 table_name="",
                 table_columns="",
                 *args, **kwargs):
        """Parses SAS file load tables
        Args:
            aws_credentials_id (str): Airflow connection ID for aws credentials
            redshift_conn_id (str): Airflow connection ID for database
            s3_bucket (str): S3 bucket name
            s3_key (srt): S3 key for file object
            aws_region (str): aws region 
            parse_value (srt): value where parsing starts
            end_string (str): value where parsing ends
            table_name (srt): Name of the target table
            table_columns (:obj:`list`): List of column names
        """

        super(SASfileToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_region = aws_region
        self.parse_value = parse_value
        self.end_string = end_string
        self.table_name = table_name
        self.table_columns = table_columns

    def execute(self, context):
        """Parses SAS file, load redshift tables.
        Args:
            context (:obj:`dict`): Dict with values to apply on content.
        Returns:
            None   
        """

        # s3 connection
        s3_conn = S3Hook(self.aws_credentials_id)

        # Redshift connection
        redshift_conn = BaseHook.get_connection(self.redshift_conn_id)
        self.log.info('Connecting to {} > > >'.format(redshift_conn.host))
        conn = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                             redshift_conn.login,
                             redshift_conn.password,
                             redshift_conn.host,
                             redshift_conn.port,
                             redshift_conn.schema
                             ))
        self.log.info("Connected to {}".format(redshift_conn.host))

        # Read file from s3
        self.log.info(
            "Reading from s3://{}/{}".format(self.s3_bucket, self.s3_key))
        file_string = s3_conn.read_key(self.s3_key, self.s3_bucket)

        # Process data
        filtered_string = file_string[file_string.index(self.parse_value):]
        filtered_string = filtered_string[:filtered_string.index(
            self.end_string)]
        
        # clean string  by removing ' and tabs
        filtered_string = filtered_string.replace("'", "").replace('\t', "")
        
        # Remove line with parse_string
        filtered_list = filtered_string.split('\n')
        filtered_list = filtered_list[1:]

        df = pd.DataFrame(filtered_list)

        df[[0, 1]] = df[0].str.split('=', n=1, expand=True)
        df[0] = df[0].str.strip()
        df[1] = df[1].str.strip()
        df[0] = df[0].str.upper()
        df[1] = df[1].str.upper()
        df = df.dropna()

        if self.table_name == 'entry_port':
            # Clean data
            df[2] = df[1].apply(lambda x: x.split(',')[0].upper())
            df[3] = df[1].apply(lambda x: x.split(',')[-1].upper())
            df[2] = df[2].str.strip()
            df[3] = df[3].str.strip()
            df[2] = df[2].str.replace(' #ARPT', "", regex=False)
            df[3] = df[3].str.replace(' (BPS)', "", regex=False)
            df[3] = df[3].str.replace(' #ARPT', "", regex=False)

            df.columns = self.table_columns

        else:

            df.columns = self.table_columns

        df.drop_duplicates().dropna()

        # Delete records from table
        self.log.info(f"Truncating dimension table {self.table_name}")
        truncate_query = text(
            SASfileToRedshiftOperator.truncate_template.format(self.table_name))
        conn.execution_options(autocommit=True).execute(truncate_query)

        # Copy data into redshift table
        self.log.info("Copying processed data from SAS file to {}, {} rows".format(
            self.table_name, len(df)))
        df.to_sql(self.table_name, conn, schema='public',
                  index=False, if_exists='append')

        conn.dispose()
