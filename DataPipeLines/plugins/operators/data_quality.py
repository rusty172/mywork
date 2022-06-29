from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 query="",
                 result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.query = query
        self.result = result      
        
        
    def execute(self, context):
        self.log.info("Getting credentials")
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        self.log.info("Running test")
        records = postgres_hook.get_records(self.query)
        if records[0][0] != self.result:
            raise ValueError(f"""
                Data quality check failed. \
                {records[0][0]} does not equal {self.result}
            """)
        else:
            self.log.info("Data quality check passed")
        
