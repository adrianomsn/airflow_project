import requests
import sqlite3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

API_KEY = "api_key"
CITY = "São Paulo"
DB_PATH = "C:\\Users\\super\\AppData\\Roaming\\DBeaverData\\workspace6\\.metadata\\sample-database-sqlite-1\\Chinook.db"
# Função para extrair dados da API OpenWeatherMap
def extrair_dados():
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()
    return data
# Função para transformar os dados
def transformar_dados():
    data = extrair_dados()
    return {
        "cidade": data["name"],
        "temperatura": data["main"]["temp"],
        "clima": data["weather"][0]["description"]
    }
# Função para armazenar no banco de dados
def carregar_dados():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clima (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cidade TEXT,
            temperatura REAL,
            clima TEXT,
            data_extracao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    data = transformar_dados()
    cursor.execute("""
        INSERT INTO clima (cidade, temperatura, clima)
        VALUES (?, ?, ?)""", (data["cidade"], data["temperatura"], data["clima"]))
    conn.commit()
    conn.close()
# Definição dos argumentos padrão do DAG
definir_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}
dag = DAG(
    'weather_pipeline',
    default_args=definir_default_args,
    schedule_interval='@daily'
)
task1 = PythonOperator(task_id='extrair', python_callable=extrair_dados, dag=dag)
task2 = PythonOperator(task_id='transformar', python_callable=transformar_dados, dag=dag)
task3 = PythonOperator(task_id='carregar', python_callable=carregar_dados, dag=dag)
task1 >> task2 >> task3
