import pandas as pd
from sqlalchemy import create_engine
import logging
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Создаем подключение к базе данных
    logging.info("Создание подключения к базе данных...")
    engine = create_engine('postgresql://postgres:08052003@localhost:5432/postgres')
    
    # Выполнение SQL-запроса для извлечения данных
    query = "SELECT * FROM dm.dm_f101_round_f"
    logging.info("Выполнение SQL-запроса...")
    # Загружаем данные в DataFrame
    df = pd.read_sql(query, engine)

    # Проверяем данные
    logging.info("Данные успешно загружены из базы данных.")
    logging.debug(f"Загруженные данные:\n{df}")
    logging.debug(f"Типы данных:\n{df.dtypes}")

    # Преобразовываем столбцы с датами в правильный формат
    if 'from_date' in df.columns:
        df['from_date'] = pd.to_datetime(df['from_date']).dt.strftime('%Y-%m-%d')
    if 'to_date' in df.columns:
        df['to_date'] = pd.to_datetime(df['to_date']).dt.strftime('%Y-%m-%d')
    
    # Приводим числовые значения к нужному формату с 8 знаками после запятой
    for col in df.select_dtypes(include=['float64', 'int64']).columns:
        df[col] = df[col].apply(lambda x: f"{x:.8f}")

    # Указываем путь для экспорта файла
    output_directory = '/Users/buh/Desktop/AIRFLOW/files'
    output_file = os.path.join(output_directory, 'dm_f101_round_f_export_2.csv')

    logging.info(f"Экспорт данных в файл {output_file}")
    df.to_csv(output_file, index=False, sep=';')

    logging.info("Данные успешно экспортированы в %s", output_file)
except Exception as e:
    logging.error("Произошла ошибка: %s", e)
finally:
    try:
        # Закрываем подключение
        engine.dispose()
        logging.info("Подключение к базе данных закрыто.")
    except Exception as e:
        logging.error("Ошибка при закрытии подключения: %s", e)

