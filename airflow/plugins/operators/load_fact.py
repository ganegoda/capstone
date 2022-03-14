from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = "#F98866"
    
    insert_sql = "INSERT INTO {} {};"
                 

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table_name = "",
                 sql_stmt= "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table_name=table_name
        self.sql_stmt=sql_stmt

            
    def execute(self, context):
 
        # Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = LoadFactOperator.insert_sql.format(self.table_name, self.sql_stmt)
        
        self.log.info(f'Loading fact table {self.table_name}..')
        redshift.run(formatted_sql)
    
        
