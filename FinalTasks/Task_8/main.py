import pyspark.sql.functions as F
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType
from clickhouse_driver import Client


def creating_data_table(table_name, columns) -> None:
    """
    Создает таблицу в db clickhouse
    :param table_name:
    :return: None
    """
    try:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            {", ".join([f'{column} {type_column.title()}' for column, type_column in columns])}
        )
        ENGINE = MergeTree()
        ORDER BY {columns[0][0]};
        """

        client.execute(create_table_query)
    except Exception as error:
        print(f"Возникла ошибка при создании таблицы {table_name}: {error}")
    else:
        print(f"Таблица {table_name} успешно создана.")


def reading_processing_data() -> None:
    """
    Читает csv файл, загружая в df, обрабатывает данные,
    настраивает типы данных, готовый результат записывает в
    db clickhouse таблица 'russian_houses'
    :return: None
    """
    try:
        # Чтение данных с .csv файла в DataFrame
        df = spark.read.csv("/opt/airflow/data/russian_houses.csv", encoding='utf-16le',
                            # /opt/airflow/data/russian_houses.csv
                            multiLine=True, header=True, inferSchema=True)

        # Вывод количества строк
        count_line = df.count()
        print(f"Количество строк: {count_line}")

        print("\nВывод схемы для проверки автоматического определения данных")
        df.printSchema()

        # Преобразование типов данных
        df = df.withColumn("house_id", df["house_id"].cast(IntegerType()))
        df = df.withColumn("maintenance_year", df["maintenance_year"].cast(IntegerType()))
        df = df.withColumn("square", df["square"].cast(DoubleType()))
        df = df.withColumn("population", df["population"].cast(IntegerType()))

        # Удаление строк с отсутствующими значениями
        df = df.dropna(how="any")
        columns = df.dtypes

        # Создание таблицы в db clickhouse
        table = "russian_houses"
        creating_data_table(table, columns)
        # Сохранение обработанных данных в таблицу 'russian_houses' db clickhouse
        df.write.jdbc(url=url, table=table, mode="append", properties=properties)

    except Exception as error:
        print(f"Возникла ошибка на этапе чтения и обработки данных: {error}")
    else:
        print(f"Обработанные данные записаны в таблицу {table}")


def query_execution() -> None:
    """
    Выполнение запросов по заданиям и запись результатов
    в таблицы db clickhouse
    :return: None
    """
    try:
        df = spark.read.jdbc(url=url, table="russian_houses", properties=properties)

        # Cредний и медианный год постройки зданий
        avg_year = df.agg(F.avg("maintenance_year").alias("average_year"))

        creating_data_table("avg_year", avg_year.dtypes)

        avg_year.write.jdbc(url=url, table="avg_year", mode="append", properties=properties)

        median_year = df.agg(F.median("maintenance_year").alias("median_year"))

        creating_data_table("median_year", median_year.dtypes)

        median_year.write.jdbc(url=url, table="median_year", mode="append", properties=properties)

        # топ-10 областей и городов с наибольшим количеством объектов
        top_regions = df.groupBy("region").agg(F.count("description").alias("count_object")).orderBy(
            F.col("count_object").desc()).limit(10)

        creating_data_table("top_regions", top_regions.dtypes)

        top_regions.write.jdbc(url=url, table="top_regions", mode="append", properties=properties)

        top_cities = df.groupBy("locality_name").agg(F.count("description").alias("count_object")).orderBy(
            F.col("count_object").desc()).limit(10)

        creating_data_table("top_cities", top_cities.dtypes)

        top_cities.write.jdbc(url=url, table="top_cities", mode="append", properties=properties)

        # Здания с максимальной и минимальной площадью в рамках каждой области
        buildings_max_min = df.groupBy("region").agg(
            F.max("square").alias("max_square"),
            F.min("square").alias("min_square")
        )

        creating_data_table("buildings_max_min", buildings_max_min.dtypes)

        buildings_max_min.write.jdbc(url=url, table="buildings_max_min", mode="append", properties=properties)

        # Количество зданий по десятилетиям
        buildings_with_decade = df.withColumn("decade", (F.col("maintenance_year") / 10).cast("int") * 10)
        building_in_yer = buildings_with_decade.groupBy("decade").agg(
            F.count("description").alias("count_description"))

        creating_data_table("building_in_yer", building_in_yer.dtypes)

        building_in_yer.write.jdbc(url=url, table="building_in_yer", mode="append", properties=properties)
    except Exception as error:
        print(f"Возникла ошибка: {error}")
    else:
        print("Запросы успешно выполнены, результаты записаны в db clickhouse по таблицам запросов")


# Настройка сессии Spark
spark = SparkSession.builder \
    .appName("MyAppRuHouses") \
    .master("local[*]") \
    .config("spark.jars", "/opt/airflow/drivers/clickhouse-jdbc-0.4.6.jar") \
    .getOrCreate()

url = "jdbc:clickhouse://172.31.0.3:8123/default"
properties = {
    "user": "default",  # Имя пользователя ClickHouse (по умолчанию "default")
    "password": "",  # Пароль (по умолчанию пустой)
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

# Настройка подключения к clickhouse
client = Client(host="clickhouse_user", port="9000")

with DAG(
        dag_id="main",
        start_date=datetime(2025, 1, 1),
        schedule_interval="@daily",
        catchup=False,
) as dag:
    task_1 = PythonOperator(
        task_id="reading_processing_data",
        python_callable=reading_processing_data,
        dag=dag
    )

    task_2 = PythonOperator(
        task_id="query_execution",
        python_callable=query_execution,
        dag=dag
    )

# Устанавливаем зависимости между задачами
task_1 >> task_2
