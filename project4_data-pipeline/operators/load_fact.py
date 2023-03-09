from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    '''
    Purpose: verifies quality of data loaded into Redshift
    Arguments:
        conn_id: Redshift connection ID
        target_table: table where data will be inserted
        select_query: SQL select query that retrieves data that need to be inserted into the previous table
    '''
    ui_color = '#F98866'

    @apply_defaults
    # Defalt parameters are defined below
    def __init__(self,
                 conn_id = "",
                 target_table = "",
                 select_query = "",
                 *args, **kwargs):

        # Now, we map the parameters
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.target_table = target_table
        self.select_query = select_query

    def execute(self, context):
        # Get Redshift credentials
        self.log.info("AWS Redshift credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)

        # We insert new data, retrieved with the select query
        self.log.info(f"Loading data into {self.target_table}")
        redshift_hook.run(f"INSERT INTO {self.target_table} {self.select_query};")