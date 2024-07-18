from pyspark.sql.functions import col, count, when, lower, trim, to_timestamp, substring

from pyspark.sql import DataFrame


class DataCleaner:
    """

    """
    def __init__(self, dataset_df: DataFrame):
        """

        :param dataset_df:
        """
        self.dataset_df = dataset_df

    def clean_data(self):
        """

        :return:
        """
        # seleziona le prime 37 colonne: nei CSV più recenti è presente un ulteriore colonna vuota da ignorare
        self.dataset_df = self.dataset_df.select(self.dataset_df.columns[:36])
        # elimina righe che hanno gli stessi valori in ogni colonna
        self.dataset_df = self.dataset_df.drop_duplicates()
        # ridenomina alcune colonne usando il CamelCase
        self.dataset_df = self.dataset_df.withColumnRenamed("STRADA1", "Strada1") \
            .withColumnRenamed("STRADA2", "Strada2") \
            .withColumnRenamed("Strada02", "Strada02") \
            .withColumnRenamed("particolaritastrade", "ParticolaritaStrade") \
            .withColumnRenamed("NUM_FERITI", "NumFeriti") \
            .withColumnRenamed("NUM_FERITI", "NumFeriti") \
            .withColumnRenamed("NUM_RISERVATA", "NumRiservata") \
            .withColumnRenamed("NUM_MORTI", "NumMorti") \
            .withColumnRenamed("NUM_ILLESI", "NumIllesi") \
            .withColumnRenamed("Tipolesione", "TipoLesione")
        # Chilometrica e DaSpecificare nel dataset contano rispettivamente 742382 e 943868 valori nulli, DecedutoDopo 921367 nulli
        # Strada2 conta 716080 nulli: Strada02 conta 257297 nulli hanno stessi valori, si può droppare Strada2
        self.dataset_df = self.remove_unnecessary_columns(["Strada2", "Chilometrica", "DaSpecificare", "DecedutoDopo"])
        self.dataset_df = self.handle_missing_values(["Localizzazione1"], "")
        self.dataset_df = self.normalize_text_data(["ParticolaritaStrade"])

        self.dataset_df = self.dataset_df.withColumn('DataOraIncidente', substring(col('DataOraIncidente'), 1, 16))
        self.dataset_df = self.dataset_df.withColumn('DataOraIncidente', to_timestamp(col('DataOraIncidente'), 'dd/MM/yyyy HH:mm'))


        return self.dataset_df

    def remove_unnecessary_columns(self, columns_to_remove):
        """

        :param columns_to_remove:
        :return:
        """
        return self.dataset_df.drop(*columns_to_remove)

    def handle_missing_values(self, columns, fill_value=None):
        """

        :param columns:
        :param fill_value:
        :return:
        """
        return self.dataset_df.na.fill(fill_value, subset=columns)

    def normalize_text_data(self, text_columns):
        """

        :param text_columns:
        :return:
        """
        for text_col in text_columns:
            self.dataset_df = self.dataset_df.withColumn(text_col, lower(trim(col(text_col))))
        return self.dataset_df



