import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

from data_preparation import DataLoader
from util import SchemaBuilder

# Configurazione di una sessione Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("MyApp") \
    .config("spark.local", "local") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()
def main():
    schema = SchemaBuilder().build_schema()
    dataset_df = DataLoader(spark, schema).load_data()

    print(f'numero di elementi del dataset: {dataset_df.count()}')

if __name__ == '__main__':
    main()
