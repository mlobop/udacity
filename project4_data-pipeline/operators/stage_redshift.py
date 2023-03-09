from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    '''
    Purpose: copies data from S3 to staging tables in Redshift
    Arguments:
        conn_id: Redshift connection ID
        aws_credentials: AWS credentials ID
        target_table: staging table where data will be copied
        s3_bucket: S3 name where JSON data are stored
        s3_subfolder: S3 subdirectory from which we will copy the data into Redshift staging table
        region: region in which data are located
        json_option: "auto" or S3 JSON path file that has the instructions to copy data
    '''
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 aws_credentials = "",
                 target_table = "",
                 s3_bucket = "",
                 s3_subfolder = "",
                 region = "",
                 json_option = "",
                 *args, **kwargs):

        # Now, we map the parameters
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.aws_credentials = aws_credentials
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_subfolder = s3_subfolder
        self.region = region
        self.json_option = json_option

    def execute(self, context):
        # Get AWS and Redshift credentials
        self.log.info("AWS and Redshift credentials")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)

        # First, we delete data from the staging table
        self.log.info(f"Clearing data from staging table {self.target_table}")
        redshift_hook.run(f"DELETE FROM {self.target_table}")

        # Now, we obtain the S3 path from which we will be copying data
        self.s3_subfolder = self.s3_subfolder.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_subfolder)

        # Finally, the copy operation is defined and performed
        self.log.info("Copying data from S3 into staging table {self.target_table}")
        redshift_hook.run(f"""COPY {self.target_table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            REGION AS '{self.region}'
            FORMAT AS JSON '{self.json_option}'
            """
        )