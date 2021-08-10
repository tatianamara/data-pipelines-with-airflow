from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 sql="",
                 target_table="",
                 append_data=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.sql=sql
        self.target_table=target_table
        self.append_data=append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_data:
            self.log.info(f"Clearing data from destination Redshift {self.target_table}")
            redshift.run(f"DELETE FROM {self.target_table}")
        
        self.log.info(f"Loading dimension table {self.target_table} to Redshift")
        insert_dim_table = f"""
                                INSERT INTO {self.target_table}
                                {self.sql}
                             """
        redshift.run(insert_dim_table)
