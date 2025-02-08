import requests
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

API_KEY = "3bd0125aa8cd0eada756ea251e8b2aa6"
CITY = "São Paulo"

# Configurações do PostgreSQL
DB_CONFIG = {
    'dbname': 'climatologia_tech',
    'user': 'seu_usuario',
    'password': 'sua_senha',
    'host': 'localhost',
    'port': '5432'
}

def extrair_dados():
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()
    return data

def transformar_dados(**context):
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids='extrair')
    return {
        "cidade": data["name"],
        "temperatura": data["main"]["temp"],
        "clima": data["weather"][0]["description"]
    }

def carregar_dados(**context):
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids='transformar')
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS clima (
                id SERIAL PRIMARY KEY,
                cidade VARCHAR(100),
                temperatura DECIMAL(5,2),
                clima VARCHAR(100),
                data_extracao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            INSERT INTO clima (cidade, temperatura, clima)
            VALUES (%s, %s, %s)""", 
            (data["cidade"], data["temperatura"], data["clima"])
        )
        
        conn.commit()
        
    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        raise e
    
    finally:
        if conn:
            cursor.close()
            conn.close()

# Definição dos argumentos padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Criação do DAG
dag = DAG(
    'postgres_weather_pipeline',
    default_args=default_args,
    description='Pipeline ETL para dados meteorológicos para banco postgres',
    schedule_interval='@daily',
    catchup=False
)

# Definição das tasks
task1 = PythonOperator(
    task_id='extrair',
    python_callable=extrair_dados,
    dag=dag,
)

task2 = PythonOperator(
    task_id='transformar',
    python_callable=transformar_dados,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id='carregar',
    python_callable=carregar_dados,
    provide_context=True,
    dag=dag,
)

# Definição do fluxo
task1 >> task2 >> task3