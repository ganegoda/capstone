from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """ Custom operator for data validation.
    Attributes:
        ui_color (str): color code for task in airflow UI
        count_template (str): Sql template for counting records
    """

    ui_color = "#89DA59"

    count_template = """
                     SELECT COUNT(*) 
                     FROM {}
                     """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 dq_test_list=[],
                 *args, **kwargs):
        """Data validation for redshift tables
        Args:
            redshift_conn_id (str): Airflow connection ID for database
            table_name (srt): Name of the target table
            dq_test_list (:obj:`dict`): dict of queries and expected results
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.dq_test_list = dq_test_list

    def execute(self, context):
        """Executes data validation tasks.
        Args:
            context (:obj:`dict`): Dict with values to apply on content.
        Returns:
            None   
        """
        # Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Starting data quality checks')

        # verify quality tests have been requested
        if len(self.dq_test_list) == 0:
            self.log.info("No quality tests provided")
            return

        # Check if tables are populated
        self.log.info(f'Fetching Record count from {self.table_name}...')
        records = redshift.get_records(
            DataQualityOperator.count_template.format(self.table_name))
        self.log.info(f'Checking if {self.table_name} table return results.')
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f'Failed! No results for {self.table_name} table')

        self.log.info(f'Checking if {self.table_name} table has records...')
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f'Failed! 0 rows in {self.table_name} table')
        self.log.info(f'Has {records[0][0]} Records!')

        failed_test_count = 0
        total_tests = len(self.dq_test_list)

        # Run quality checks
        for test in self.dq_test_list:

            test_sql = test.get("test_sql")
            expected_result = test.get("expected_result")

            try:
                self.log.info(f"Running test {test_sql}")
                records = redshift.get_records(test_sql)[0][0]
                if records != expected_result:
                    self.log.info(
                        f"Data quality test failed. Expected {expected_result}, received {records}")
                    failed_test_count += 1

            except Exception as err:
                self.log.info(f"test {test_sql} failed with exception {err}")

        if failed_test_count > 0:
            self.log.info(f"{failed_test_count}/{total_tests} tests failed")
            raise ValueError("Data quality checks failed!")

        self.log.info("All data quality tests passed!")
