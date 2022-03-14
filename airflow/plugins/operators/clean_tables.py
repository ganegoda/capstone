from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CleanTablesOperator(BaseOperator):

    ui_color = "#80BD9E"
                 

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table_name = "",
                 sql_stmt="",
                 *args, **kwargs):

        super(CleanTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table_name=table_name
        self.sql_stmt=sql_stmt

    def execute(self, context):
        
        # Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

          
        self.log.info(f'Cleaning table {self.table_name}..')
        redshift.run(self.sql_stmt)
        
