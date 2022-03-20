from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """ Custom operator for loading fact table.
    Attributes:
        ui_color (str): color code for task in airflow UI
        insert_sql (str): Sql template for inserting records
    """

    ui_color = "#F98866"
    
    insert_sql = "INSERT INTO {} {};"
            
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 sql_stmt="",
                 *args, **kwargs):

        """Load fact table
        Args:
            redshift_conn_id (str): Airflow connection ID for database
            table_name (srt): Name of the target table
            sql_stmt (str): sql statement for loading fact table
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_stmt = sql_stmt
            
    def execute(self, context):
        """Executes data insert tasks.
        Args:
            context (:obj:`dict`): Dict with values to apply on content.
        Returns:
            None   
        """
        # Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = LoadFactOperator.insert_sql.format(self.table_name, self.sql_stmt)
        
        self.log.info(f'Loading fact table {self.table_name}..')
        redshift.run(formatted_sql)
    
        
