from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("MyHomework") \
    .config("spark.master", "local[*]") \
    .getOrCreate()

spark.createDataFrame()

authors_df = spark.read.options(header=True, inferSchema=True).csv('authors.csv')
books_df = spark.read.options(header=True, inferSchema=True).csv('books.csv')

# authors_df.printSchema()
# authors_df.show()
#
# books_df.printSchema()
# books_df.show()

authors_df.withColumn('birth_date', F.to_date('birth_date', "yyyy-MM-DD"))
books_df.withColumn('publish_date', F.to_date('publish_date', "yyyy-MM-DD"))

library_df = authors_df.join(books_df, "author_id")

# Найдите топ-5 авторов, книги которых принесли наибольшую выручку.
top_price_df = library_df.groupBy("author_id", "name").agg(F.sum("price").alias("total_price")) \
    .orderBy(F.col("total_price").desc()).limit(5)

# Найдите количество книг в каждом жанре.
count_book_in_genre_df = library_df.groupBy("genre").agg(F.count("title").alias("count")).orderBy(
    F.col("count").desc())
# Подсчитайте среднюю цену книг по каждому автору.
avg_price = library_df.groupBy("author_id", "name").agg(F.avg("price").alias("avg_price")) \
    .orderBy(F.col("avg_price").desc())
#Найдите книги, опубликованные после 2000 года, и отсортируйте их по цене.
books_last_2000 = library_df.filter(F.year("publish_date") >= 2000).show()

spark.stop()
