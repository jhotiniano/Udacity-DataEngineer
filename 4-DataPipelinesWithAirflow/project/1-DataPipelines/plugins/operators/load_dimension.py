from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self, redshift_connection = "", table = "", sql_query_variable = "", append_only = False, *args, **kwargs):
        """
        This function is the constructor of the class.
        It contains all the variables to handle.
        
        INPUT:
        * self the variable that refers to the current instance of the class
        * redshift_connection the connection to the redshift cluster
        * table the existing table in the redshift cluster
        * sql_query_variable the query to be executed in the redshift cluster
        * append_only append only
        * *args the context information handler
        * **kwargs the context information handler
        """
        
        # 
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        # Variable initialization
        self.redshift_connection = redshift_connection
        self.table = table
        self.sql_query_variable = sql_query_variable
        self.append_only = append_only

    def execute(self, context):
        """
        This function connects to the redshift cluster.
        Then clean and insert data to the corresponding table.
        
        INPUT:
        * self the variable that refers to the current instance of the class
        * context the context information
        """
        
        # reference to redshift cluster
        redshift = PostgresHook(postgres_conn_id = self.redshift_connection)
        
        # clean table content and execute query
        if not self.append_only:
            redshift.run("DELETE FROM {}".format(self.table))
        formatted_sql = getattr(SqlQueries, self.sql_query_variable).format(self.table)
        redshift.run(formatted_sql)