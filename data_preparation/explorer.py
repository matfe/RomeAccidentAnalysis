from pyspark.sql.functions import count, when, isnan, col

from util import AnalysisOutputSaver

class DataAnalysisExplorer:
    """
    Classe per eseguire l'analisi esplorativa dei dati.
    """

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def explore_data(self):
        self.describe_data()
        self.count_missing_values()
        self.show_unique_values(["NaturaIncidente","ParticolaritaStrade","TipoStrada","FondoStradale"])

    def describe_data(self):
        """
        Calcola e salva in un file CSV le statistiche descrittive per il dataframe.
        """
        dataframe_description = self.dataframe.describe()
        AnalysisOutputSaver().save_html(dataframe_description, "dataframe_description")

    def count_missing_values(self):
        """
        Conta il numero di valori mancanti per ogni colonna nel dataframe.
        """
        missing_values = self.dataframe.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in self.dataframe.columns])
        AnalysisOutputSaver().save_html(missing_values, "missing_values")

    def show_unique_values(self, variables):
        """
        Mostra i valori unici per una colonna specifica.
        """
        unique_values = self.dataframe.select(variables[0]).distinct()
        AnalysisOutputSaver().save_html(unique_values, "unique_values")