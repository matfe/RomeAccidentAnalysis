import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

from util import SchemaBuilder

from data_preparation import DataLoader
from data_preparation import DataCleaner
from data_preparation import DataAnalysisExplorer
from data_analysis import AccidentAnalysis
from data_analysis import AccidentGeoAnalysis
from util import AnalysisOutputSaver

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

    cleaned_dataset_df = DataCleaner().clean_data(dataset_df)

    cleaned_dataset_df.show(truncate=False)

    DataAnalysisExplorer().explore_data(cleaned_dataset_df)

if __name__ == '__main__':
    main()
