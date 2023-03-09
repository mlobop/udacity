from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    '''
    Purpose: verifies quality of data loaded into Redshift
    Arguments:
        conn_id: Redshift connection ID
        quality_checks: dict that contains a SQL query, the expected result and the name of the table
    '''
    ui_color = '#89DA59'

    @apply_defaults
    # Defalt parameters are defined below
    def __init__(self,
                 conn_id = "",
                 quality_checks = "",
                 *args, **kwargs):

        # Now, we map the parameters
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.quality_checks = quality_checks

    def execute(self, context):
        # Get Redshift credentials
        self.log.info("AWS Redshift credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)

        for q_check in self.quality_checks:
            # We define and execute the required test in the songplays table
            self.log.info(f"Running test on {q_check['tab_name']} table")
            records = redshift_hook.get_records(q_check['test_sql'])

            # Raise an exception in case that the table contains no results
            if records[0][0] != q_check['expected_result']:
                raise ValueError(f"Data quality check failed. {q_check['tab_name']} table contains {records[0][0]} null rows")
            else:
                self.log.info(f"Data quality check passed on {q_check['tab_name']} table")