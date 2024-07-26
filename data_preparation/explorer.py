from pyspark.sql.functions import count, when, isnan, col

from util import AnalysisOutputSaver
from util import ConfigLoader

class DataAnalysisExplorer:
    """
    Classe per eseguire l'analisi esplorativa dei dati.
    """

    def __init__(self, config_path='./config/application.json'):
        # Carica le configurazioni dal file di configurazione
        self.config = ConfigLoader(config_path).load_config()
        self.quantitative_variables = self.config['variables']['qualitative']

    def explore_data(self, dataframe):
        """
        Esegue un insieme di task di analisi per esplorare i dati.

        I risultati delle analisi vengono salvati in diversi file HTML
        """
        self.describe_data(dataframe)
        self.count_missing_values(dataframe)
        self.show_unique_values(dataframe, self.quantitative_variables)

    def describe_data(self, dataframe):
        """
        Calcola e salva in un file CSV le statistiche descrittive per il dataframe.
        """
        dataframe_description = dataframe.describe()
        dataframe_description.select("summary", "Protocollo", "NumFeriti", "NumMorti","NumIllesi").show()
        AnalysisOutputSaver().save_html(dataframe_description, "dataframe_description")

    def count_missing_values(self, dataframe):
        """
        Conta il numero di valori mancanti per ogni colonna nel dataframe.
        """
        missing_values = dataframe.select([count(when(col(c).isNull(), c)).alias(c) for c in dataframe.columns])
        missing_values.show()
        AnalysisOutputSaver().save_html(missing_values, "missing_values")

    def show_unique_values(self, dataframe, variables):
        """
        Elenca i valori unici per una colonna specifica.
        """
        for variable in variables:
            unique_values = dataframe.select(variable).distinct()
            unique_values.show()
            AnalysisOutputSaver().save_html(unique_values, f'unique_values_{variable}')