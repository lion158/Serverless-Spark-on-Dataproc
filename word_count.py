import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, size, sum
import time

table = "bigquery-public-data:github_repos.sample_contents"

spark = SparkSession.builder \
          .appName("pyspark-example") \
          .config("spark.jars","gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar") \
          .getOrCreate()

read_start = time.time()
df = spark.read.format("bigquery").load(table)
read_end = time.time()
read_time = read_end - read_start
print(f"Read time: {read_time}")

result_start = time.time()
# Dodaj nową kolumnę z liczbą słów
df = df.withColumn("word_count", size(split(col("content"), ' ')))

# Zlicz łączną liczbę wszystkich słów
total_count = df.agg(sum("word_count")).collect()[0][0]

result_end = time.time()
result_time = result_end - result_start
print(f"Result time: {result_time}")

# Wyświetl wynik
print(f"Łączna liczba wszystkich słów: {total_count}")
