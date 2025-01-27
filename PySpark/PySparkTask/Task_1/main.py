from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, mean, month, year, when

# Создание SparkSession
spark = SparkSession.builder.appName("WeatherAnalysis").getOrCreate()

# Чтение данных
weather_df = spark.read.csv("weather_data.csv", header=True, inferSchema=True)

# Преобразование столбца date в формат даты
weather_df = weather_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

# Заполнение пропущенных значений средними значениями по метеостанциям
mean_temp = weather_df.groupBy("station_id").agg(mean("temperature").alias("mean_temp"))
mean_precip = weather_df.groupBy("station_id").agg(mean("precipitation").alias("mean_precip"))
mean_wind = weather_df.groupBy("station_id").agg(mean("wind_speed").alias("mean_wind"))

weather_df = weather_df.join(mean_temp, on="station_id", how="left")
weather_df = weather_df.join(mean_precip, on="station_id", how="left")
weather_df = weather_df.join(mean_wind, on="station_id", how="left")

weather_df = weather_df.withColumn("temperature",
                                   when(col("temperature").isNull(), col("mean_temp")).otherwise(col("temperature")))
weather_df = weather_df.withColumn("precipitation",
                                   when(col("precipitation").isNull(), col("mean_precip")).otherwise(col("precipitation")))
weather_df = weather_df.withColumn("wind_speed",
                                   when(col("wind_speed").isNull(), col("mean_wind")).otherwise(col("wind_speed")))

# Удаление временных столбцов
weather_df = weather_df.drop("mean_temp", "mean_precip", "mean_wind")

# Топ-5 самых жарких дней
hottest_days = weather_df.orderBy(col("temperature").desc()).select("date", "temperature").limit(5)
hottest_days.show()

# Метеостанция с наибольшим количеством осадков за последний год
last_year = weather_df.filter(year("date") == 2023)
station_precip = last_year.groupBy("station_id").sum("precipitation").orderBy(col("sum(precipitation)").desc()).limit(1)
station_precip.show()

# Средняя температура по месяцам
monthly_avg_temp = weather_df.withColumn("month", month("date")).groupBy("month").avg("temperature").orderBy("month")
monthly_avg_temp.show()