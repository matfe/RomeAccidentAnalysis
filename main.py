import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

from util import SchemaBuilder
from util import AnalysisOutputSaver
from data_preparation import DataLoader
from data_preparation import DataCleaner
from data_preparation import DataAnalysisExplorer
from data_analysis import AccidentGeoAnalysis
from data_analysis import AccidentAnalysis
from graph_analysis import GraphAnalysis
from graph_analysis import AccidentPeopleGraph
from graph_analysis import AccidentCrossroadsGraph

# Configurazione di una sessione Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("MyApp") \
    .config("spark.local", "local") \
    .config("spark.driver.host", "localhost") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .getOrCreate()

def main():

    # Costruisce lo schema del dataset.
    schema = SchemaBuilder().build_schema()

    # Carica i dati utilizzando lo schema e crea un DataFrame Spark.
    dataset_df = DataLoader(spark, schema).load_data()

    # Pulisce il dataset.
    cleaned_dataset_df = DataCleaner().clean_data(dataset_df)

    # Memorizza il DataFrame in memoria.
    cleaned_dataset_df = cleaned_dataset_df.cache()

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

    # Ottiene le combinazioni demografiche da analizzare.
    demographic_combinations = AccidentAnalysis().get_demographic_combinations_for_analysis()

    # Salva la distribuzione dei dati demografici nel dataset pulito.
    AnalysisOutputSaver().save_plot_distribution(cleaned_dataset_df, demographic_combinations)

    # Crea il grafo di incroci stradali con incidenti.
    accidentCrossroadsGraph, nameAccidentCrossroadsGraph = AccidentCrossroadsGraph(cleaned_dataset_df).graph()

    # Crea il grafo di persone coinvolte in incidenti.
    accidentPeopleGraph, nameAccidentPeopleGraph = AccidentPeopleGraph(cleaned_dataset_df).graph()

    # Analizza il grafo di incidenti avvenuti agli incroci stradali.
    GraphAnalysis.analyze(accidentCrossroadsGraph, nameAccidentCrossroadsGraph)

    # Analizza il grafo di persone coinvolte in incidenti.
    GraphAnalysis.analyze(accidentPeopleGraph, nameAccidentPeopleGraph, sampleForAdvancedAnalysis=True)


if __name__ == '__main__':
    main()
