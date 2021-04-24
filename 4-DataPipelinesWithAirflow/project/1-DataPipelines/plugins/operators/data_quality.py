from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, redshift_connection = "", checks = [], *args, **kwargs):
        """
        This function is the constructor of the class.
        It contains all the variables to handle.
        
        INPUT:
        * self the variable that refers to the current instance of the class
        * redshift_connection the connection to the redshift cluster
        * checks the check list to pass data quality
        * *args the context information handler
        * **kwargs the context information handler
        """
        
        # 
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        # Variable initialization
        self.redshift_connection = redshift_connection
        self.checks = checks

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
        for check in self.checks:
            records = redshift.get_records(check['check_sql'])
            if len(records) < 0 or len(records[0]) < 0:
                self.log.error(f"{check['table']} returned no results")
                raise ValueError(f"Data quality check failed. {check['table']} returned no results")
            num_records = records[0][0]
            if num_records > check['expected_result']:
                self.log.error(f"Records present NULL in destination table {check['table']} on ID {check['id_table']}")
                raise ValueError(f"Records present NULL in destination {check['table']} on ID {check['id_table']}")
            self.log.info(f"Data quality on table {check['table']} check passed")