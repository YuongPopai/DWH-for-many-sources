import logging
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def generate_settlement_report(**kwargs):
    target_conn_id = 'PG_WAREHOUSE_CONNECTION'
    pg_conn = BaseHook.get_connection(target_conn_id)

    try:
        with psycopg2.connect(pg_conn.get_uri()) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date,
                                                          orders_count, orders_total_sum,
                                                          orders_bonus_payment_sum, orders_bonus_granted_sum,
                                                          order_processing_fee, restaurant_reward_sum)
                    SELECT
                        r.restaurant_id,
                        r.restaurant_name,
                        DATE_TRUNC('month', MAX(t.ts)) AS settlement_date,
                        COUNT(DISTINCT o.id) AS orders_count,
                        GREATEST(SUM(ps.total_sum), 0) AS orders_total_sum,
                        GREATEST(SUM(ps.bonus_payment), 0) AS orders_bonus_payment_sum,
                        GREATEST(SUM(ps.bonus_grant), 0) AS orders_bonus_granted_sum,
                        GREATEST(SUM(ps.total_sum) * 0.25, 0) AS order_processing_fee,
                        GREATEST((SUM(ps.total_sum) - SUM(ps.bonus_payment) - (SUM(ps.total_sum) * 0.25)), 0) AS restaurant_reward_sum
                    FROM
                        dds.dm_orders o
                    JOIN
                        dds.dm_restaurants r ON o.restaurant_id = r.id
                    JOIN
                        dds.fct_product_sales ps ON o.id = ps.order_id
                    JOIN
                        dds.dm_timestamps t ON o.timestamp_id = t.id
                    WHERE
                        o.order_status = 'CLOSED'
                        AND t.ts >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month'
                        AND t.ts < date_trunc('month', CURRENT_DATE)
                    GROUP BY
                        r.restaurant_id, r.restaurant_name
                    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
                    SET
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = GREATEST(EXCLUDED.orders_total_sum, 0),
                        orders_bonus_payment_sum = GREATEST(EXCLUDED.orders_bonus_payment_sum, 0),
                        orders_bonus_granted_sum = GREATEST(EXCLUDED.orders_bonus_granted_sum, 0),
                        order_processing_fee = GREATEST(EXCLUDED.order_processing_fee, 0),
                        restaurant_reward_sum = GREATEST(EXCLUDED.restaurant_reward_sum, 0);
                """)

                conn.commit()

    except Exception as e:
        log.error(f"Ошибка при генерации отчета по расчетам: {e}")
        raise

def generate_courier_ledger(**kwargs):
    target_conn_id = 'PG_WAREHOUSE_CONNECTION'
    pg_conn = BaseHook.get_connection(target_conn_id)

    try:
        with psycopg2.connect(pg_conn.get_uri()) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month,
                                                         orders_count, orders_total_sum, rate_avg,
                                                         order_processing_fee, courier_order_sum, courier_tips_sum,
                                                         courier_reward_sum)
                    SELECT 
                        t1.courier_id,
                        t1.courier_name,
                        t4.year,
                        t4."month",
                        COUNT(*) AS orders_count,
                        SUM(t2."sum") AS orders_total_sum,
                        AVG(t2.rate) AS rate_avg,
                        SUM(t2."sum") * 0.25 AS order_processing_fee,
                        CASE 
                            WHEN AVG(t2.rate) < 4 THEN 
                                GREATEST(0.05 * SUM(t2."sum"), 100) 
                            WHEN AVG(t2.rate) >= 4 AND AVG(t2.rate) < 4.5 THEN 
                                GREATEST(0.07 * SUM(t2."sum"), 150) 
                            WHEN AVG(t2.rate) >= 4.5 AND AVG(t2.rate) < 4.9 THEN 
                                GREATEST(0.08 * SUM(t2."sum"), 175) 
                            ELSE 
                                GREATEST(0.10 * SUM(t2."sum"), 200) 
                        END AS courier_order_sum,
                        SUM(t2.tip_sum) AS courier_tips_sum,
                        SUM(t2."sum") + SUM(t2.tip_sum) * 0.95 AS courier_reward_sum
                    FROM 
                        dds.dm_couriers t1
                    INNER JOIN 
                        dds.dm_delivers t2 ON t2.courier_id = t1.id
                    INNER JOIN 
                        dds.dm_orders t3 ON t2.order_id = t3.id 
                    INNER JOIN 
                        dds.dm_timestamps t4 ON t3.timestamp_id = t4.id
                    WHERE 
                        t4.ts >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month'
                        AND t4.ts < date_trunc('month', CURRENT_DATE)
                    GROUP BY 
                        t1.courier_id, t1.courier_name, t4.year, t4."month"
                    ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
                    SET
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = GREATEST(EXCLUDED.orders_total_sum, 0),
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = GREATEST(EXCLUDED.order_processing_fee, 0),
                        courier_order_sum = GREATEST(EXCLUDED.courier_order_sum, 0),
                        courier_tips_sum = GREATEST(EXCLUDED.courier_tips_sum, 0),
                        courier_reward_sum = GREATEST(EXCLUDED.courier_reward_sum, 0);
                """)

                conn.commit()

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
