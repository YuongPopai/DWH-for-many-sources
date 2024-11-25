import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import json
from typing import Any, Dict

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def json2str(obj: Any) -> str:
    return json.dumps(to_dict(obj), sort_keys=True, ensure_ascii=False)

def str2json(json_str: str) -> Dict:
    return json.loads(json_str)

def to_dict(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(obj, dict):
        data = {}
        for k, v in obj.items():
            data[k] = to_dict(v)
        return data
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [to_dict(v) for v in obj]
    else:
        return obj

def get_connection():
    target_conn_id = 'PG_WAREHOUSE_CONNECTION'
    pg_conn = BaseHook.get_connection(target_conn_id)
    return psycopg2.connect(pg_conn.get_uri(), cursor_factory=RealDictCursor)

def load_restaurants(**kwargs):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''SELECT object_value FROM stg.ordersystem_restaurants UNION SELECT object_value FROM stg.api_restaurants;''')
                rows = cursor.fetchall()

                for row in rows:
                    restaurant_data = str2json(row['object_value'])

                    restaurant_id = restaurant_data.get('_id')
                    restaurant_name = restaurant_data.get('name')
                    active_from = datetime.now()
                    active_to = datetime(2099, 12, 31, 0, 0, 0) 

                    cursor.execute(
                        """
                        INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (restaurant_id) DO UPDATE
                        SET
                            restaurant_name = EXCLUDED.restaurant_name,
                            active_from = EXCLUDED.active_from,
                            active_to = EXCLUDED.active_to;
                        """,
                        (restaurant_id, restaurant_name, active_from, active_to)
                    )
                
                conn.commit()
                log.info(f"Загружено {len(rows)} записей")
    
    except Exception as e:
        log.error(f"Ошибка при загрузке ресторанов: {e}")
        raise

def load_users(**kwargs):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT object_value FROM stg.ordersystem_users;")
                rows = cursor.fetchall()

                for row in rows:
                    user_data = str2json(row['object_value'])

                    user_id = user_data.get('_id')
                    user_name = user_data.get('name')
                    user_login = user_data.get('login')

                    cursor.execute(
                        """
                        INSERT INTO dds.dm_users (user_id, user_name, user_login)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE
                        SET 
                            user_name = EXCLUDED.user_name,
                            user_login = EXCLUDED.user_login;
                        """,
                        (user_id, user_name, user_login)
                    )
                
                conn.commit()
                log.info(f"Загружено {len(rows)} записей")
    
    except Exception as e:
        log.error(f"Ошибка при загрузке пользователей: {e}")
        raise

def load_curiers(**kwargs):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''SELECT object_value FROM stg.api_couriers;''')
                rows = cursor.fetchall()

                for row in rows:
                    curier_data = str2json(row['object_value'])

                    courier_id = curier_data.get('_id')
                    courier_name = curier_data.get('name')

                    cursor.execute(
                        """
                        INSERT INTO dds.dm_couriers (courier_id, courier_name)
                        VALUES (%s, %s)
                        ON CONFLICT (courier_id) DO NOTHING;
                        """,
                        (courier_id, courier_name)
                    )
                
                conn.commit()
                log.info(f"Загружено {len(rows)} записей")
    
    except Exception as e:
        log.error(f"Ошибка при загрузке курьеров: {e}")
        raise
def load_timestamps(**kwargs):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT object_value FROM stg.ordersystem_orders;")
                rows = cursor.fetchall()

                for row in rows:
                    order_data = str2json(row['object_value'])
                    
                    if order_data.get('final_status') in ['CLOSED', 'CANCELLED']:
                        order_date = order_data.get('date')
                        
                        ts = datetime.fromisoformat(order_date)
                        year = ts.year
                        month = ts.month
                        day = ts.day
                        date = ts.date()
                        time = ts.time()
                        cursor.execute(
                            """
                            INSERT INTO dds.dm_timestamps (ts, year, month, day, time, date)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (ts) DO NOTHING;
                            """,
                            (ts, year, month, day, time, date)
                        )
                
                conn.commit()
                log.info(f"Загружено {len(rows)} записей")
    
    except Exception as e:
        log.error(f"Ошибка при загрузке временных меток: {e}")
        raise

def load_products(**kwargs):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, restaurant_id FROM dds.dm_restaurants;")
                restaurant_rows = cursor.fetchall()
                restaurant_mapping = {row['restaurant_id']: row['id'] for row in restaurant_rows}   
                
                cursor.execute("SELECT object_value, update_ts FROM stg.ordersystem_restaurants;")
                product_rows = cursor.fetchall()
                
                for row in product_rows:
                    restaurant_data = str2json(row['object_value'])
                    update_ts = row['update_ts']
                    menu_items = restaurant_data.get('menu', [])
                    object_id = restaurant_data.get('_id')
                    restaurant_id = restaurant_mapping.get(object_id)

                    if restaurant_id is None:
                        continue

                    for item in menu_items:
                        product_id = item['_id']
                        product_name = item['name']
                        product_price = item['price']
                        active_from = update_ts
                        active_to = datetime(2099, 12, 31)

                        cursor.execute(
                            """
                            INSERT INTO dds.dm_products (restaurant_id, product_id, product_name, product_price, active_from, active_to)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (product_name, restaurant_id) DO NOTHING;
                            """,
                            (restaurant_id, product_id, product_name, product_price, active_from, active_to)
                        )
                
                conn.commit()
                log.info(f"Загружено {len(product_rows)} записей")
    
    except Exception as e:
        log.error(f"Ошибка при загрузке продуктов: {e}")
        raise

def load_orders(**kwargs):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT object_value, update_ts FROM stg.ordersystem_orders;")
                order_rows = cursor.fetchall()

                for row in order_rows:
                    order_data = str2json(row['object_value'])
                    update_ts = row['update_ts']
                    restaurant_object_id = order_data['restaurant']['id']
                    user_object_id = order_data['user']['id']
                    order_key = order_data['_id']
                    final_status = order_data['final_status']

                    cursor.execute(
                        "SELECT id FROM dds.dm_restaurants WHERE restaurant_id = %s;",
                        (restaurant_object_id,)
                    )
                    restaurant_id_row = cursor.fetchone()
                    restaurant_id = restaurant_id_row['id'] if restaurant_id_row else None

                    cursor.execute(
                        "SELECT id FROM dds.dm_users WHERE user_id = %s;",
                        (user_object_id,)
                    )
                    user_id_row = cursor.fetchone()
                    user_id = user_id_row['id'] if user_id_row else None
                    order_date = order_data['date']
                    cursor.execute(
                        "SELECT id FROM dds.dm_timestamps WHERE ts = %s;",
                        (order_date,)
                    )
                    timestamp_row = cursor.fetchone()
                    timestamp_id = timestamp_row['id'] if timestamp_row else None

                    if not all([restaurant_id, user_id, timestamp_id]):
                        continue

                    cursor.execute(
                        """
                        INSERT INTO dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (order_key) DO NOTHING;
                        """,
                        (user_id, restaurant_id, timestamp_id, order_key, final_status)
                    )
                
                conn.commit()
                log.info(f"Загружено {len(order_rows)} записей")
    
    except Exception as e:
        log.error(f"Ошибка при загрузке заказов: {e}")
        raise

def load_product_sales(**kwargs):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT object_value FROM stg.ordersystem_orders;")
                order_rows = cursor.fetchall()

                for row in order_rows:
                    order_data = str2json(row['object_value'])
                    order_key = order_data['_id']  
                    order_items = order_data.get('order_items', [])
                    bonus_payment = order_data.get('bonus_payment', 0)
                    bonus_grant = order_data.get('bonus_grant', 0)

                    cursor.execute(
                        "SELECT id FROM dds.dm_orders WHERE order_key = %s;",
                        (order_key,)
                    )
                    order_id_row = cursor.fetchone()
                    order_id = order_id_row['id'] if order_id_row else None

                    if order_id is None:
                        continue

                    for item in order_items:
                        product_object_id = item['id']
                        count = item['quantity']
                        price = item['price']
                        total_sum = count * price

                        cursor.execute(
                            "SELECT id FROM dds.dm_products WHERE product_id = %s;",
                            (product_object_id,)
                        )
                        product_row = cursor.fetchone()
                        if product_row is None:
                            continue

                        product_id = product_row['id']

                        cursor.execute(
                            """
                            INSERT INTO dds.fct_product_sales (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (product_id, order_id) DO NOTHING;
                            """,
                            (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                        )

                conn.commit()
                log.info(f"Загружено {len(order_rows)} записей")

    except Exception as e:
        log.error(f"Ошибка при загрузке данных по продажам продуктов: {e}")
        raise

def load_delivers(**kwargs):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''SELECT object_value FROM stg.api_delivers;''')
                rows = cursor.fetchall()
                
                for row in rows:
                    delivery_data = str2json(row['object_value'])

                    order_id = delivery_data.get("order_id")
                    courier_id = delivery_data.get("courier_id")
                    cursor.execute('''SELECT id FROM dds.dm_orders WHERE order_key = %s;''', (order_id,))
                    order_record = cursor.fetchone()
                    order_id_db = order_record['id'] if order_record else None

                    cursor.execute('''SELECT id FROM dds.dm_couriers WHERE courier_id = %s;''', (courier_id,))
                    courier_record = cursor.fetchone()
                    courier_id_db = courier_record['id'] if courier_record else None

                    if order_id_db is None or courier_id_db is None:
                        log.warning(f"Не удалось найти order_id или courier_id для данных: {delivery_data}")
                        continue

                    cursor.execute(
                        """
                        INSERT INTO dds.dm_delivers (delivery_id, order_id, courier_id, address, order_ts, delivery_ts, rate, sum, tip_sum)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (delivery_id) DO NOTHING;
                        """,
                        (
                            delivery_data.get("delivery_id"),
                            order_id_db,
                            courier_id_db,
                            delivery_data.get("address"),
                            delivery_data.get("order_ts"),
                            delivery_data.get("delivery_ts"),
                            delivery_data.get("rate"),
                            delivery_data.get("sum"),
                            delivery_data.get("tip_sum")
                        )
                    )
                
                conn.commit()
                log.info(f"Загружено {len(rows)} записей")

    except Exception as e:
        log.error(f"Ошибка при загрузке доставок: {e}")
        raise

dag = DAG(
    dag_id='fill_dds_dag',
    schedule_interval='0/15 * * * *',
    start_date=datetime(2024, 11, 21),
)

load_product_sales_task = PythonOperator(
    task_id='load_product_sales',
    python_callable=load_product_sales,
    dag=dag,
)
load_orders_task = PythonOperator(
    task_id='load_orders',
    python_callable=load_orders,
    dag=dag,
)

load_products_task = PythonOperator(
    task_id='load_products',
    python_callable=load_products,
    dag=dag,
)
load_timestamps_task = PythonOperator(
    task_id='load_timestamps',
    python_callable=load_timestamps,
    dag=dag,
)
load_users_task = PythonOperator(
    task_id='load_users',
    python_callable=load_users,
    dag=dag,
)

load_restaurants_task = PythonOperator(
    task_id='load_restaurants',
    python_callable=load_restaurants,
    dag=dag,
)
load_curiers_task = PythonOperator(
    task_id='load_curiers',
    python_callable=load_curiers,
    dag=dag,
)
load_delivers_task = PythonOperator(
    task_id='load_delivers',
    python_callable=load_delivers,
    dag=dag,
)


[load_restaurants_task, load_users_task, load_timestamps_task, load_curiers_task] >> load_products_task >> load_orders_task >> [load_product_sales_task, load_delivers_task]
