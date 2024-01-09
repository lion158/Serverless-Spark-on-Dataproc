import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
import random

    
if len(sys.argv) < 3:
    print("Please provide both a GCS bucket name and size.")

bucket = sys.argv[1]
size = int(sys.argv[2])


# Inicjalizacja sesji Spark
spark = SparkSession.builder.appName("GenerateRandomDataset").getOrCreate()

# Definicja funkcji generującej losowe liczby w zakresie 1-100
def generate_random_number():
    return random.randint(1, 100)

# Zdefiniowanie funkcji UDF (User-Defined Function) w Spark SQL
generate_random_number_udf = spark.udf.register("generate_random_number_udf", generate_random_number, IntegerType())

# Liczba rekordów potrzebna do osiągnięcia 10GB (10^10 bajtów)
target_size_bytes = size * 1024**3
num_records = target_size_bytes // 4  # Zakładamy, że każda liczba to 4 bajty (int)

# Generowanie DataFrame z losowymi liczbami
df = spark.range(1, num_records + 1).withColumn("random_number", generate_random_number_udf())

# Zapisanie DataFrame do pliku CSV
df.write.csv(f"gs://{bucket}/random_numbers_dataset.csv", header=True, mode="overwrite")

# Zakończenie sesji Spark
spark.stop()
