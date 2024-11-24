import logging
from bson import ObjectId
from pymongo import MongoClient
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from psycopg2.extras import RealDictCursor
import pendulum
import psycopg2
import json
from urllib.parse import quote_plus as quote
from datetime import datetime, timedelta
import requests
from pymongo.collection import Collection



API_BASE_URL = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
NICKNAME = 'NIKITOSIK'  
COHORT_NUMBER = '26'  
API_KEY = '25c27781-8fde-4b30-a22e-524044a7580f'  


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

class MongoConnect:
    def __init__(self, cert_path: str, user: str, pw: str, host: str, rs: str, auth_db: str, main_db: str) -> None:
        self.user = user
        self.pw = pw
        self.hosts = [host]
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    def url(self) -> str:
        return f'mongodb://{quote(self.user)}:{quote(self.pw)}@{",".join(self.hosts)}/?replicaSet={self.replica_set}&authSource={self.auth_db}'

    def client(self) -> Collection:
        return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db]

def serialize_mongo_object(mongo_obj):
    if isinstance(mongo_obj, ObjectId):
        return str(mongo_obj)
    if isinstance(mongo_obj, datetime):
        return mongo_obj.isoformat()  
    if isinstance(mongo_obj, dict):
        return {k: serialize_mongo_object(v) for k, v in mongo_obj.items()}
    if isinstance(mongo_obj, list):
        return [serialize_mongo_object(item) for item in mongo_obj]
    return mongo_obj

def load_and_save_orders(**kwargs):
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
    orders_collection = mongo_connect.client().orders

    orders = list(orders_collection.find({}))

    target_conn_id = 'PG_WAREHOUSE_CONNECTION'
    target_conn = BaseHook.get_connection(target_conn_id)

    with psycopg2.connect(target_conn.get_uri()) as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                SELECT workflow_settings
                FROM stg.srv_wf_settings
                WHERE workflow_key = 'origin_to_stg_orders';
            ''')
            result = cursor.fetchone()
            last_loaded_ts = None

            if result and isinstance(result[0], dict):
                workflow_settings = result[0]  
            elif result and isinstance(result[0], str):
                workflow_settings = json.loads(result[0])  
            else:
                workflow_settings = {} 

            last_loaded_ts_str = workflow_settings.get("last_loaded_ts")
            if last_loaded_ts_str and last_loaded_ts_str.lower() != "null":
                last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)


            for order in orders:
                object_id = str(order['_id'])  
                update_ts = order.get('update_ts')
                serialized_order = serialize_mongo_object(order)
                object_value_str = json.dumps(serialized_order, ensure_ascii=False)

                if isinstance(update_ts, str):
                    update_ts = datetime.fromisoformat(update_ts)

                if last_loaded_ts is not None and update_ts < last_loaded_ts:
                    continue

                cursor.execute('''
                    INSERT INTO stg.ordersystem_orders (object_id, object_value, update_ts)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                ''', (object_id, object_value_str, update_ts))

            conn.commit()

            if orders:
                last_restaurant_update_ts = max(order.get('update_ts', datetime.min) for order in orders)
                new_last_loaded_ts = last_restaurant_update_ts.isoformat()
                new_workflow_settings = {
                    "last_loaded_ts": new_last_loaded_ts
                }

                cursor.execute('''
                    UPDATE stg.srv_wf_settings
                    SET workflow_settings = %s
                    WHERE workflow_key = 'origin_to_stg_orders';
                ''', (json.dumps(new_workflow_settings),))

def load_and_save_restaurants(**kwargs):
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
    restaurants_collection = mongo_connect.client().restaurants

    restaurants = list(restaurants_collection.find({}))


    target_conn_id = 'PG_WAREHOUSE_CONNECTION'
    target_conn = BaseHook.get_connection(target_conn_id)

    with psycopg2.connect(target_conn.get_uri()) as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                SELECT workflow_settings
                FROM stg.srv_wf_settings
                WHERE workflow_key = 'origin_to_stg_restaurants';
            ''')
            result = cursor.fetchone()
            last_loaded_ts = None

            if result and isinstance(result[0], dict):
                workflow_settings = result[0]  
            elif result and isinstance(result[0], str):
                workflow_settings = json.loads(result[0])  
            else:
                workflow_settings = {}

            last_loaded_ts_str = workflow_settings.get("last_loaded_ts")  
            if last_loaded_ts_str and last_loaded_ts_str.lower() != "null": 
                last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)


            last_restaurant_update_ts = None

            for restaurant in restaurants:
                object_id = str(restaurant['_id'])  
                update_ts = restaurant.get('update_ts')
                serialized_restaurant = serialize_mongo_object(restaurant)
                object_value_str = json.dumps(serialized_restaurant, ensure_ascii=False)

                if isinstance(update_ts, str):
                    update_ts = datetime.fromisoformat(update_ts)

                if last_loaded_ts is not None and update_ts < last_loaded_ts:
                    continue

                cursor.execute('''
                    INSERT INTO stg.ordersystem_restaurants (object_id, object_value, update_ts)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                ''', (object_id, object_value_str, update_ts))

                last_restaurant_update_ts = update_ts 

            conn.commit()

            if last_restaurant_update_ts is not None:
                new_last_loaded_ts = last_restaurant_update_ts.isoformat()
                new_workflow_settings = {
                    "last_loaded_ts": new_last_loaded_ts
                }

                cursor.execute('''
                    UPDATE stg.srv_wf_settings
                    SET workflow_settings = %s
                    WHERE workflow_key = 'origin_to_stg_restaurants';
                ''', (json.dumps(new_workflow_settings),))

def load_and_save_users(**kwargs):
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
    users_collection = mongo_connect.client().users

    users = list(users_collection.find({}))

    target_conn_id = 'PG_WAREHOUSE_CONNECTION'
    target_conn = BaseHook.get_connection(target_conn_id)

    with psycopg2.connect(target_conn.get_uri()) as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                SELECT workflow_settings
                FROM stg.srv_wf_settings
                WHERE workflow_key = 'origin_to_stg_users';
            ''')
            result = cursor.fetchone()
            last_loaded_ts = None

            if result and isinstance(result[0], dict):
                workflow_settings = result[0]  
            elif result and isinstance(result[0], str):
                workflow_settings = json.loads(result[0])  
            else:
                workflow_settings = {}  

            last_loaded_ts_str = workflow_settings.get("last_loaded_ts")
            if last_loaded_ts_str and last_loaded_ts_str.lower() != "null":
                last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)

            for user in users:
                object_id = str(user['_id'])  
                update_ts = user.get('update_ts')
                serialized_user = serialize_mongo_object(user)
                object_value_str = json.dumps(serialized_user, ensure_ascii=False)

                if isinstance(update_ts, str):
                    update_ts = datetime.fromisoformat(update_ts)

                if last_loaded_ts is not None and update_ts < last_loaded_ts:
                    continue

                cursor.execute('''
                    INSERT INTO stg.ordersystem_users (object_id, object_value, update_ts)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                ''', (object_id, object_value_str, update_ts))

            conn.commit()

            if users:
                last_user_update_ts = max(user.get('update_ts', datetime.min) for user in users)
                new_last_loaded_ts = last_user_update_ts.isoformat()
                new_workflow_settings = {
                    "last_loaded_ts": new_last_loaded_ts
                }

                cursor.execute('''
                    UPDATE stg.srv_wf_settings
                    SET workflow_settings = %s
                    WHERE workflow_key = 'origin_to_stg_users';
                ''', (json.dumps(new_workflow_settings),))


def load_events(**kwargs):
    target_conn_id = 'PG_WAREHOUSE_CONNECTION'
    source_conn = BaseHook.get_connection(target_conn_id)

    with psycopg2.connect(source_conn.get_uri()) as src_conn:
        with src_conn.cursor(cursor_factory=RealDictCursor) as src_cursor:
            src_cursor.execute('''
                SELECT MAX(data::int) FROM (
                    SELECT (workflow_settings::JSON->>'last_loaded_id') AS data 
                    FROM stg.srv_wf_settings
                    where workflow_key = %s
                ) t1;
            ''', ['save_last_id'])
            result = src_cursor.fetchone()
            max_id = result['max']
            src_cursor.execute(f'SELECT * FROM public.outbox WHERE object_id > %s;', (max_id,))
            rows = src_cursor.fetchall()

            for row in rows:
                src_cursor.execute('''INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                    VALUES (%s, %s, %s, %s);''', 
                    (row['object_id'], row['record_ts'], row['type'], row['payload']))
            
            src_cursor.execute('SELECT MAX(id) FROM stg.bonussystem_events;')
            max_id = src_cursor.fetchone()['max']
            
            src_cursor.execute('''UPDATE stg.srv_wf_settings
                SET workflow_settings = jsonb_set(workflow_settings::jsonb, '{last_loaded_id}', %s::jsonb)
                WHERE workflow_key = 'example_ranks_origin_to_stg_workflow';''', (json.dumps(max_id),))
def load_ranks(**kwargs):
    source_conn_id = 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION'
    target_conn_id = 'PG_WAREHOUSE_CONNECTION'
    source_conn = BaseHook.get_connection(source_conn_id)
    target_conn = BaseHook.get_connection(target_conn_id)

    try:
        with psycopg2.connect(source_conn.get_uri()) as src_conn:
            with src_conn.cursor(cursor_factory=RealDictCursor) as src_cursor:
                src_cursor.execute("SELECT * FROM public.ranks;")
                ranks_rows = src_cursor.fetchall()

        with psycopg2.connect(target_conn.get_uri()) as tgt_conn:
            with tgt_conn.cursor() as tgt_cursor:
                for row in ranks_rows:
                    tgt_cursor.execute(
                        """
                        INSERT INTO stg.bonussystem_ranks (id, name, bonus_percent, min_payment_threshold)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE
                        SET
                            name = EXCLUDED.name,
                            bonus_percent = EXCLUDED.bonus_percent,
                            min_payment_threshold = EXCLUDED.min_payment_threshold;
                        """,
                        (row['id'], row['name'], row['bonus_percent'], row['min_payment_threshold'])
                    )
                log.info(f"Загружено {len(ranks_rows)} записей")
                tgt_conn.commit()  

    except Exception as e:
        log.error(f"Ошибка при загрузке данных: {e}")
        raise
def load_users(**kwargs):
    source_conn_id = 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION'
    target_conn_id = 'PG_WAREHOUSE_CONNECTION'

    source_conn = BaseHook.get_connection(source_conn_id)
    target_conn = BaseHook.get_connection(target_conn_id)

    try:
        with psycopg2.connect(source_conn.get_uri()) as src_conn:
            with src_conn.cursor(cursor_factory=RealDictCursor) as src_cursor:
                src_cursor.execute("SELECT * FROM public.users;")
                users_rows = src_cursor.fetchall()

        with psycopg2.connect(target_conn.get_uri()) as tgt_conn:
            with tgt_conn.cursor() as tgt_cursor:
                for row in users_rows:
                    tgt_cursor.execute(
                        """
                        INSERT INTO stg.bonussystem_users (id, order_user_id)
                        VALUES (%s, %s)
                        ON CONFLICT (id) DO UPDATE
                        SET
                            order_user_id = EXCLUDED.order_user_id;
                        """,
                        (row['id'], row['order_user_id'])
                    )

                log.info(f"Загружено {len(users_rows)} записей")
                tgt_conn.commit()  

    except Exception as e:
        log.error(f"Ошибка при загрузке данных: {e}")
        raise
def load_api_couriers(**kwargs): 
    target_conn_id = 'PG_WAREHOUSE_CONNECTION'
    pg_conn = BaseHook.get_connection(target_conn_id)
    conn = psycopg2.connect(
        host=pg_conn.host,
        port=pg_conn.port,
        database=pg_conn.schema,
        user=pg_conn.login,
        password=pg_conn.password
    )
    cursor = conn.cursor()

    def get_couriers(sort_field='id', sort_direction='asc', limit=50, offset=50):
        url = f'{API_BASE_URL}/couriers'
        
        params = {
            'sort_field': sort_field,
            'sort_direction': sort_direction,
            'limit': limit,
            'offset': offset
        }
        
        headers = {
            'X-Nickname': NICKNAME,
            'X-Cohort': COHORT_NUMBER,
            'X-API-KEY': API_KEY,
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            return response.json()  
        else:
            logging.error(f'Ошибка {response.status_code}: {response.text}') 
            return []

    offset = 0
    while True:
        couriers = get_couriers(limit=50, offset=offset)
        if not couriers:
            break 
        
        for item in couriers:
            object_id = item['_id']
            if 'name' in item:
                decoded_name = item['name'] 
                item['name'] = decoded_name
            
            object_value = json.dumps(item, ensure_ascii=False)  
            
            cursor.execute(
                """
                INSERT INTO stg.api_couriers (object_id, object_value)
                VALUES (%s, %s)
                ON CONFLICT (object_id) DO NOTHING;  
                """,
                (object_id, object_value)
            )
        
        conn.commit()  
        offset += 50 

    cursor.close()
    conn.close()

def load_api_restaurants(**kwargs):

    
    target_conn_id = 'PG_WAREHOUSE_CONNECTION'
    pg_conn = BaseHook.get_connection(target_conn_id)
    conn = psycopg2.connect(
        host=pg_conn.host,
        port=pg_conn.port,
        database=pg_conn.schema,
        user=pg_conn.login,
        password=pg_conn.password
    )
    cursor = conn.cursor()
    
    def get_restaurants(sort_field='id', sort_direction='asc', limit=50, offset=50):
        url = f'{API_BASE_URL}/restaurants'
        
        params = {
            'sort_field': sort_field,
            'sort_direction': sort_direction,
            'limit': limit,
            'offset': offset
        }
        
        headers = {
            'X-Nickname': NICKNAME,
            'X-Cohort': COHORT_NUMBER,
            'X-API-KEY': API_KEY,
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            return response.json()  
        else:
            logging.error(f'Ошибка {response.status_code}: {response.text}') 
            return []

    offset = 50
    while True:
        restaurants = get_restaurants(limit=50, offset=offset)
        if not restaurants:
            break 
        
        for item in restaurants:
            object_id = item['_id']
            decoded_name = item['name'] if 'name' in item else None
            
            object_value = json.dumps(item, ensure_ascii=False) 
            
            cursor.execute(
                """
                INSERT INTO stg.api_restaurants (object_id, object_value)
                VALUES (%s, %s)
                ON CONFLICT (object_id) DO NOTHING;  
                """,
                (object_id, object_value)
            )
        
        conn.commit()  
        offset += 50 

    cursor.close()
    conn.close()

def load_api_deliveries(**kwargs):
    target_conn_id = 'PG_WAREHOUSE_CONNECTION'
    pg_conn = BaseHook.get_connection(target_conn_id)

    conn = psycopg2.connect(
        host=pg_conn.host,
        port=pg_conn.port,
        database=pg_conn.schema,
        user=pg_conn.login,
        password=pg_conn.password
    )
    cursor = conn.cursor()

    from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')

    def get_deliveries(from_date, sort_field='_id', sort_direction='asc', limit=50, offset=50):
        url = f'{API_BASE_URL}/deliveries'
        
        params = {
            'from': from_date,
            'sort_field': sort_field,
            'sort_direction': sort_direction,
            'limit': limit,
            'offset': offset
        }

        headers = {
            'X-Nickname': NICKNAME,
            'X-Cohort': COHORT_NUMBER,
            'X-API-KEY': API_KEY,
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            return response.json()  
        else:
            logging.error(f'Ошибка {response.status_code}: {response.text}')
            return []

    offset = 50
    while True:
        deliveries = get_deliveries(from_date=from_date, limit=50, offset=offset)
        if not deliveries:
            break  
        
        for item in deliveries:
            order_id = item['delivery_id']
            delivery_data = json.dumps(item, ensure_ascii=False)
            
            cursor.execute(
                """
                INSERT INTO stg.api_delivers (object_id, object_value)
                VALUES (%s, %s)
                ON CONFLICT (object_id) DO NOTHING;  
                """,
                (order_id, delivery_data)
            )
        
        conn.commit()  
        offset += 50  

    cursor.close()
    conn.close()
dag = DAG(
    dag_id='gg',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
)
load_deliveries = PythonOperator(
    task_id='load_delivers',
    python_callable=load_api_deliveries,
    dag=dag,
)
load_restaurants_api = PythonOperator(
    task_id='load_restaurants',
    python_callable=load_api_restaurants,
    dag=dag,
)
load_couriers = PythonOperator(
    task_id='load_curiers',
    python_callable=load_api_couriers,
    dag=dag,
)
load_mongo_orders = PythonOperator(
    task_id='load_mongo_orders',
    python_callable=load_and_save_orders,
    dag=dag
)

load_mongo_restaurants = PythonOperator(
    task_id='load_mongo_restaurants',
    python_callable=load_and_save_restaurants,
    dag=dag
)

load_mongo_users = PythonOperator(
    task_id='load_mongo_users',
    python_callable=load_and_save_users,
    dag=dag
)
load_postgres_events = PythonOperator(
    task_id='load_events',
    python_callable=load_events,
    dag=dag,
)
load_postgres_users = PythonOperator(
    task_id='load_users',
    python_callable=load_users,
    dag=dag,
)

load_postgres_ranks = PythonOperator(
    task_id='load_ranks',
    python_callable=load_ranks,
    dag=dag,
)

load_deliveries >> load_restaurants_api >> load_couriers >> load_mongo_orders >> load_mongo_restaurants >> load_mongo_users >> load_postgres_events >> load_postgres_users >> load_postgres_ranks
