from pyspark.sql.functions import col, count, when, lower, trim, to_timestamp, substring

from pyspark.sql import DataFrame

from util import ConfigLoader

class DataCleaner:
    """
    Classe per la pulizia e la trasformazione dei dati di un DataFrame.
    """
    def __init__(self, config_path='./config/application.json'):
        # Carica le configurazioni dal file di configurazione
        self.config = ConfigLoader(config_path).load_config()
        self.qualitative_variables = self.config['variables']['qualitative']
        self.quantitative_variables = self.config['variables']['quantitative']
        self.geographic_variables = self.config['variables']['geographic']
        self.drop_variables = self.config['drop']
        self.rename_variables = self.config['rename']['old_new']
        self.values_to_replace = self.config['values_to_replace']

    def clean_data(self, dataset_df):
        """
        Pulisce e trasforma completa del DataFrame.

        Richiama i metodi per rinominare le colonne, sostituire i valori nulli, normalizzare il testo delle colonne e rimuovere le colonne specificate.

        :return: Il DataFrame aggiornato
        """

        # Seleziona le prime 37 colonne: nei CSV più recenti è presente un ulteriore colonna vuota da ignorare
        dataset_df = dataset_df.select(dataset_df.columns[:36])

        # Elimina righe che hanno gli stessi valori in ogni colonna
        dataset_df = dataset_df.drop_duplicates()
        dataset_df = self.rename_columns(dataset_df)
        """
        Alcune colonne sono state rimosse perchè contengono un numero elevato di valori nulli.
        
        Ad esempio:
        Chilometrica e DaSpecificare hanno rispettivamente 742382 e 943868 valori nulli, mentre DecedutoDopo ha 921367 nulli
        Strada2 ha 716080 nulli e Strada02 ha 257297 nulli: hanno stessi valori, quindi si può droppare Strada2
        """
        dataset_df = self.remove_unnecessary_columns(dataset_df, self.drop_variables)
        dataset_df = self.handle_missing_values(dataset_df, self.quantitative_variables, 0)
        dataset_df = self.normalize_text_data(dataset_df, self.qualitative_variables)
        dataset_df = self.normalize_text_data(dataset_df, self.geographic_variables)

        # Tronca la colonna 'DataOraIncidente' ai primi 16 caratteri, convertendola in formato timestamp 'dd/MM/yyyy HH
        dataset_df = dataset_df.withColumn('DataOraIncidente', substring(col('DataOraIncidente'), 1, 16))  \
                               .withColumn('DataOraIncidente', to_timestamp(col('DataOraIncidente'), 'dd/MM/yyyy HH:mm'))

        # Sostituisce i valori nel DataFrame secondo il dizionario self.values_to_replace.
        dataset_df = self.replace_values(dataset_df)

        return dataset_df

    def rename_columns(self, dataset_df):
        """
        Rinomina le colonne del DataFrame in base a una mappatura specificata.

        :param dataset_df: Il DataFrame da elaborare.
        :return: Il DataFrame con le colonne rinominate.
        """
        for old_name, new_name in self.rename_variables.items():
            dataset_df = dataset_df.withColumnRenamed(old_name, new_name)
        return dataset_df

    def remove_unnecessary_columns(self, dataset_df, columns_to_remove):
        """
        Rimuove un elenco di colonne specificate dal DataFrame.

        :param dataset_df: Il DataFrame da elaborare
        :param columns_to_remove: Lista delle colonne da rimuovere
        :return: DataFrame aggiornato
        """
        return dataset_df.drop(*columns_to_remove)

    def handle_missing_values(self, dataset_df, columns, fill_value=None):
        """
        Esegue la sostituzione dei valori nulli con fill_value in un elenco di colonne del DataFrame.

        :param dataset_df: Il DataFrame da elaborare
        :param columns: Lista delle colonne in cui sostituire i valori nulli
        :param fill_value: Il valore con cui sostituire i valori nulli
        :return: DataFrame aggiornato
        """
        return dataset_df.na.fill(fill_value, subset=columns)

    def normalize_text_data(self, dataset_df, text_columns):
        """
        Esegue operazioni di normalizzazione sul testo di una lista di colonne del DataFrame.

        :param dataset_df: Il DataFrame da elaborare
        :param text_columns: Lista delle colonne di testo da normalizzare
        :return: DataFrame aggiornato
        """
        for text_col in text_columns:
            dataset_df = dataset_df.withColumn(text_col, lower(trim(col(text_col))))
        return dataset_df

    def replace_values(self, dataset_df: DataFrame) -> DataFrame:
        """
        Sostituisce i valori nel DataFrame secondo il dizionario self.values_to_replace.

        :param dataset_df: DataFrame in cui eseguire le sostituzioni
        :return: DataFrame con valori sostituiti
        """
        for key, value in self.values_to_replace.items():
            replacement_list, default_value = value

            if key in dataset_df.columns:
                dataset_df = dataset_df.replace(replacement_list, default_value, subset=[key])

        return dataset_df





