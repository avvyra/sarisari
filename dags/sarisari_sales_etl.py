from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from pipeline import extract, load, quality_check, transform


@dag(dag_id='sarisari_sales_etl', start_date=datetime(2026, 10, 1),
     schedule=None, catchup=False, tags=['sari-sari', 'etl'])
def sales_etl():
    @task(task_id='extract')
    def extract_task():
        return extract(get_current_context()['run_id'])

    @task(task_id='transform')
    def transform_task(raw_path):
        return transform(raw_path, get_current_context()['run_id'])

    @task(task_id='load')
    def load_task(staged_path):
        load(staged_path)

    @task(task_id='quality_check')
    def check_task():
        quality_check()

    # The arguments create extract -> transform -> load; this sets the last arrow.
    check_task().set_upstream(load_task(transform_task(extract_task())))


sales_etl()
