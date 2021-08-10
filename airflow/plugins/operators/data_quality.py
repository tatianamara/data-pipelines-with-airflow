from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables_to_validate = "",
                 expected_result = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.tables_to_validate = tables_to_validate
        self.expected_result = expected_result

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables_to_validate:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < self.expected_result or len(records[0]) < self.expected_result:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < self.expected_result:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        self.log.info(f"Data quality check passed in all tables")