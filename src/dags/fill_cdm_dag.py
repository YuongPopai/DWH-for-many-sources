import logging
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def get_connection_and_cursor():
    target_conn_id = 'PG_WAREHOUSE_CONNECTION'
    pg_conn = BaseHook.get_connection(target_conn_id)
    
    conn = psycopg2.connect(pg_conn.get_uri())
    cursor = conn.cursor()
    return conn, cursor

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def execute_sql(query):
    with get_connection_and_cursor() as (conn, cursor):
        cursor.execute(query)
        conn.commit()

def generate_settlement_report(**kwargs):
    sql_file_path = os.path.join(os.path.dirname(__file__), 'dml_scripts', 'dml_dm_settlement_report.sql')
    query = read_sql_file(sql_file_path)
    try:
        execute_sql(query)
    except Exception as e:
        log.error(f"Ошибка при генерации отчета по расчетам: {e}")
        raise

def generate_courier_ledger(**kwargs):
    sql_file_path = os.path.join(os.path.dirname(__file__), 'dml_scripts', 'dml_dm_courier_ledger.sql')
    query = read_sql_file(sql_file_path)
    try:
        execute_sql(query)
    except Exception as e:
        log.error(f"Ошибка при генерации отчета по курьеру: {e}")
        raise

dag = DAG(
    dag_id='fill_cdm_dag',
    schedule_interval='0 0 1 * *',
    start_date=datetime(2024, 11, 21),
    catchup=False,
)

generate_report_task = PythonOperator(
    task_id='generate_settlement_report',
    python_callable=generate_settlement_report,
    dag=dag,
)

generate_courier_ledger_task = PythonOperator(
    task_id='generate_courier_ledger',
    python_callable=generate_courier_ledger,
    dag=dag,
)

generate_report_task >> generate_courier_ledger_task
