from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    # bulk data load script. 
    # Requires credentials and file format.
    sql_query = """
        COPY {} FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {} 'auto';
    """

    @apply_defaults
    def __init__(self, redshift_connection = "", amazon_conecction_user = "", table = "", s3_path = "", region = "us-west-2", format_file = "", *args, **kwargs):
        """
        This function is the constructor of the class.
        It contains all the variables to handle.
        
        INPUT:
        * self the variable that refers to the current instance of the class
        * redshift_connection the connection to the redshift cluster 
        * amazon_conecction_user the connection to the aws user account
        * table the existing table in the redshift cluster
        * s3_path the path in the s3 bucket
        * region the bucket region in s3
        * format_file the data file format
        * *args the context information handler
        * **kwargs the context information handler
        """
        
        # 
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        # Variable initialization
        self.redshift_connection = redshift_connection
        self.amazon_conecction_user=amazon_conecction_user
        self.table=table
        self.s3_path=s3_path
        self.region=region
        self.format_file=format_file
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        """
        This function connects to s3 and redshift.
        Then remove and run a bulk data load script (copy).
        
        INPUT:
        * self the variable that refers to the current instance of the class
        * context the context information
        """
        
        # reference to user connection aws (gets credentials) and redshift cluster
        aws = AwsHook(self.amazon_conecction_user)
        credentials = aws.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_connection)
        
        # clean table content
        redshift.run("DELETE FROM {}".format(self.table))
        
        # run the query defined at the top of the class
        formatted_sql = StageToRedshiftOperator.sql_query.format(
            self.table
            ,self.s3_path
            ,credentials.access_key
            ,credentials.secret_key
            ,self.region
            ,self.format_file
            ,self.execution_date
        )
        redshift.run(formatted_sql)