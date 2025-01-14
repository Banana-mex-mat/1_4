from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.configuration import conf
from airflow.models import Variable
import pandas as pd
import os
from datetime import datetime

# Путь к файлу из переменной Airflow:
PATH = Variable.get("my_path")
conf.set("core", "template_searchpath", PATH)

# Функция импорта данных
def import_data(table_name):
    # Формируем полный путь к файлу
    file_path = f"{PATH}/dm_f101_round_f_export_2.csv"
    
    # Проверяем, существует ли файл
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файла {file_path} не существует.")
    
    # Читаем CSV файл
    df = pd.read_csv(file_path, delimiter=";")
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema="dm", if_exists="replace", index=False)

default_args = {
    "owner": "irina",
    "start_date": datetime(2024, 2, 25),
    "retries": 2
} 
# Создаем DAG
with DAG(
    "import_data",
    default_args=default_args,
    description="Загрузка данных в dm",
    catchup=False,
    template_searchpath = [PATH],
    schedule="0 0 * * *" 
) as dag:
    start = DummyOperator( 
        task_id = "start" )
    
    import_dm_f101_round_f_v2 = PythonOperator(
        task_id="import_dm_f101_round_f_v2",
        python_callable=import_data,
        op_kwargs={"table_name" : "dm_f101_round_f_v2"}
    )

    end = DummyOperator(
        task_id = "end"
    )
    (
        start
        >> import_dm_f101_round_f_v2
        >> end
    )

