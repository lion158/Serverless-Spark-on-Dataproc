import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time 

if len(sys.argv) == 1:
    print("Please provide a GCS bucket name.")

bucket = sys.argv[1]

# Inicjalizacja sesji Spark
spark = SparkSession.builder.appName("CountOccurrences").getOrCreate()

read_start = time.time()
# Wczytanie danych z pliku CSV
df = spark.read.csv(f"gs://{bucket}/random_numbers_dataset.csv", header=True, inferSchema=True)
read_end = time.time()
read_time = read_end - read_start
print(f"Read time: {read_time}")


result_start = time.time()
# Zliczenie wystąpień każdej liczby
result = df.groupBy("random_number").count()
result_end = time.time()
result_time = result_end - result_start
print(f"Result time: {read_time}")

# Wydruk wyników
result.show()

result.write.option("header", True).csv(f"gs://{bucket}/numbers_count")

# Zakończenie sesji Spark
spark.stop()
