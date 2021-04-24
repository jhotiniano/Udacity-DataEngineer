from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, redshift_connection = "", tables = [], *args, **kwargs):
        """
        This function is the constructor of the class.
        It contains all the variables to handle.
        
        INPUT:
        * self the variable that refers to the current instance of the class
        * redshift_connection the connection to the redshift cluster
        * tables the existing tables in the redshift cluster
        * *args the context information handler
        * **kwargs the context information handler
        """
        
        # 
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        # Variable initialization
        self.redshift_connection = redshift_connection
        self.tables = tables

    def execute(self, context):
        """
        This function contains a connection to the redshift cluster.
        Then, perform record count validation.
        
        INPUT:
        * self the variable that refers to the current instance of the class
        * context the context information
        """
        
        # reference to redshift cluster
        redshift = PostgresHook(postgres_conn_id = self.redshift_connection)    
        
        # validates the number of records and zeros values in each table 
        for table in self.tables:
            records = redshift.get_records("SELECT count(*) FROM {}".format(table))        
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. {} returned no results".format(table))
            num_records = records[0][0]
            if num_records == 0:
                raise ValueError("No records present in destination {}".format(table))
            self.log.info("Data quality on table {} check passed with {} records".format(table, num_records))