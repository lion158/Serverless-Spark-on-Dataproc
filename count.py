import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if len(sys.argv) == 1:
    print("Please provide a GCS bucket name.")

bucket = sys.argv[1]

# Inicjalizacja sesji Spark
spark = SparkSession.builder.appName("CountOccurrences").getOrCreate()

# Wczytanie danych z pliku CSV
df = spark.read.csv("random_numbers_dataset.csv", header=True, inferSchema=True)

# Zliczenie wystąpień każdej liczby
result = df.groupBy("random_number").count()

# Wydruk wyników
result.show()

result.write.option("header", True).csv(f"gs://{bucket}/numbers_count")

# Zakończenie sesji Spark
spark.stop()
