from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CleanTablesOperator(BaseOperator):
    """ Custom operator for cleaning data tables.
    Attributes:
        ui_color (str): color code for task in airflow UI
    """
    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 sql_stmt="",
                 *args,  **kwargs):
        """ Executes data cleanup queries.
        Args:
        redshift_conn_id: Airflow connection ID for aws redshift database
        table_name: Name of the target table
        sql_stmt: Sql queries to run
        """

        super(CleanTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_stmt = sql_stmt

    def execute(self, context):
        """Executes task for cleaning tables.
        Args:
            context (:obj:`dict`): Dict with values to apply on content.
        Returns:
            None   
        """
        # Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Cleaning table {self.table_name}..')
        redshift.run(self.sql_stmt)
