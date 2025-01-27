from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder\
.appName("SparkHomework")\
.config("spark.master", "local[*]")\
.getOrCreate()

web_server_logs_df = spark.read.options(header=True, inferSchema=True).csv('web_server_logs.csv')

#1. Сгруппируйте данные по IP и посчитайте количество запросов для каждого IP, выводим 10 самых активных IP.
print("Top 10 active IP addresses:")
top_ip_df = web_server_logs_df.groupBy("ip")\
    .agg(F.count("method").alias("request_count"))\
    .orderBy(F.col("request_count").desc()).limit(10)\
    .show()

#2. Сгруппируйте данные по HTTP-методу и посчитайте количество запросов для каждого метода.
print("Request count by HTTP method:")
count_by_http_df = web_server_logs_df.groupBy("method")\
    .agg(F.count("method").alias("method_count"))\
    .show()

#3. Профильтруйте и посчитайте количество запросов с кодом ответа 404.
response_code_count = web_server_logs_df.filter(F.col("response_code") == 404).count()
print("Number of 404 response code: {}".format(response_code_count))

#4. Сгруппируйте данные по дате и просуммируйте размер ответов, сортируйте по дате.
print("Total response size by day:")
response_size_by_day_df = web_server_logs_df\
    .withColumn("timestamp", F.to_date("timestamp", "'yyyy-MM-dd'"))\
    .groupBy("timestamp").agg(F.sum("response_size").alias("total_response_size"))\
    .orderBy("timestamp").show()

spark.stop()