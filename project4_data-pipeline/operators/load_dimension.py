from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    '''
    Purpose: inserts new data into a dimension table in Redshift
    Arguments:
        conn_id: Redshift connection ID
        target_table: table where data will be inserted
        select_query: SQL select query that retrieves data that need to be inserted into the previous table
        truncate_or_append: boolean that allows the user to switch between deleting rows or just appending records to the table
    '''
    ui_color = '#80BD9E'

    @apply_defaults
    # Defalt parameters are defined below
    def __init__(self,
                 conn_id = "",
                 target_table = "",
                 select_query = "",
                 truncate_or_append = False,
                 *args, **kwargs):

        # Now, we map the parameters
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.target_table = target_table
        self.select_query = select_query
        self.truncate_or_append = truncate_or_append

    def execute(self, context):
        # Get Redshift credentials
        self.log.info("AWS Redshift credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)

        # First, the rows of the dimension table are deleted
        # This happens if the truncate_or_append argument is set to True
        if self.truncate_or_append:
            self.log.info(f"Clearing data from {self.target_table}")
            redshift_hook.run(f"TRUNCATE TABLE {self.target_table};")

        # Then, we insert new data, retrieved with the select query
        self.log.info(f"Loading data into {self.target_table}")
        redshift_hook.run(f"INSERT INTO {self.target_table} {self.select_query};")