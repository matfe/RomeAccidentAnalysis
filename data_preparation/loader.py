import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from util import ConfigLoader
from util import PathBuilder


class DataLoader:
    """
    Classe per  caricare dati da diversi file CSV sia locali che in HDFS.

    :var spark: una sessione Spark per gestire le operazioni con Spark
    :var dataset_path: il percorso del file da cui caricare i dati
    :var schema: lo schema da utilizzare per leggere i dati
    """

    def __init__(self, spark: SparkSession, schema: StructType, config_path='./config/loader_config.json'):
        """
        Inizializza una nuova istanza della classe DataLoader con uno schema definito.

        :param spark: una sessione Spark per gestire le operazioni con Spark
        :param dataset_path: il percorso al dataset da caricare
        :param schema: lo schema Spark SQL per definire la struttura dei dati del DataFrame.
        """
        self.spark = spark
        self.schema = schema

        # Carica le configurazioni dal file di configurazione
        self.config = ConfigLoader(config_path).load_config()
        self.base_path = self.config['base_path']
        self.file_type = self.config['type_file']
        self.pathBuilder = PathBuilder(self.base_path)

    def load_data(self):
        """
        Carica un DataFrame leggendo file CSV del dataset, situati nel percorso specificato utilizzando lo schema fornito.

        :return: DataFrame caricato se esistono file validi, altrimenti None.
        """
        if self.file_type.lower() == 'local':
            dataset_df = (self.spark.read.option("delimiter", ";").csv(self.pathBuilder.csv_path, schema=self.schema, header=True))
            dataset_df.printSchema()
            dataset_df.show()
            return dataset_df

        elif self.file_type.lower() == 'hdfs':
            raise ValueError(f"Unsupported file type {self.file_type}")
        else:
            raise ValueError(f"Unsupported file type {self.file_type}")