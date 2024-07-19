import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

from util import SchemaBuilder

from data_preparation import DataLoader
from data_preparation import DataCleaner
from data_preparation import DataAnalysisExplorer
from data_analysis import AccidentGeoAnalysis
from data_analysis import AccidentAnalysis
from util import AnalysisOutputSaver

# Configurazione di una sessione Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("MyApp") \
    .config("spark.local", "local") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

def main():

    # Costruisce lo schema del dataset.
    schema = SchemaBuilder().build_schema()

    # Carica i dati utilizzando lo schema e crea un DataFrame Spark.
    dataset_df = DataLoader(spark, schema).load_data()

    # Pulisce il dataset.
    cleaned_dataset_df = DataCleaner().clean_data(dataset_df)

    # Esplora i dati puliti per ottenere informazioni preliminari e statistiche descrittive.
    DataAnalysisExplorer().explore_data(cleaned_dataset_df)

    # Esegue l'analisi del dataset pulito e restituisce i risultati.
    results = AccidentAnalysis().analyze(cleaned_dataset_df)

    # Stampa i risultati dell'analisi.
    for key, result in results.items():
        print(f" ----------------- {key}  -----------------")
        result.show()

    # Salva i plot basati sui risultati dell'analisi.
    AnalysisOutputSaver().save_plots(results)

    # Crea e salva le mappe geografiche degli incidenti utilizzando il dataset pulito.
    AccidentGeoAnalysis().plot_maps(cleaned_dataset_df)

    """
    # Ottiene le combinazioni demografiche da analizzare.
    demographic_combinations = AccidentAnalysis().get_demographic_combinations_for_analysis()

    # Salva la distribuzione dei dati demografici nel dataset pulito.
    AnalysisOutputSaver().save_plot_distribution(cleaned_dataset_df, demographic_combinations)
    """

if __name__ == '__main__':
    main()
