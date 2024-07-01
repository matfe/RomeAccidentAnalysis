from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class DataLoader:
    """
    Classe per caricare dati da un percorso specificato utilizzando Spark e uno schema definito.

    :param spark: una sessione Spark per gestire le operazioni con Spark
    :param dataset_path: il percorso del file da cui caricare i dati
    :param schema: lo schema da utilizzare per leggere i dati
    """

    def __init__(self, spark: SparkSession, dataset_path: str, schema: StructType):
        """
        Inizializza una nuova istanza della classe DataLoader con uno schema definito.

        :param spark: una sessione Spark per gestire le operazioni con Spark
        :param dataset_path: il percorso al dataset da caricare
        :param schema: lo schema Spark SQL per definire la struttura dei dati del DataFrame.
        """
        self.dataset_path = dataset_path
        self.spark = spark
        self.schema = schema

    def load_data(self):
        """
        Carica un DataFrame da file CSV situati nel percorso specificato utilizzando lo schema fornito.

        :return: il DataFrame caricato se esistono file validi, altrimenti None.
        """
        try:
            dataset_df = self.spark.read.csv(self.dataset_path, schema=self.schema, header=True)
            print("Creato il dataframe del dataset con lo schema fornito")
            dataset_df.printSchema()
            return dataset_df
        except Exception as e:
            print(f"Error loading data: {e}")
            return None