from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_query = load_query

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run("""
            INSERT INTO {}
            {}
            """.format(self.table, self.load_query)
        )
