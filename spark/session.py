import os

from pyspark.sql import SparkSession

from util import ConfigLoader


class SparkSessionBuilder:
    """
    Classe che crea e gestisce una sessione Spark.

    Due opzioni per configurare il master per eseguire Spark:
        - "local[*]": esegue Spark localmente utilizzando tutti i core disponibili. Suggerito per lo sviluppo e il debug.
        - "spark://localhost:7077": connessione a un cluster Spark standalone in esecuzione su localhost.
    """

    def __init__(self, config_path='./config/spark.json'):
        config = ConfigLoader(config_path).load_config()

        self.builder = SparkSession.builder \
            .appName(config['app_name']) \
            .config("spark.driver.host", config.get('driver_host', 'localhost'))

        if config['mode'] == "local":
            self.builder = self.builder.master("local[*]") \
                .config("spark.local", "local")
        elif config['mode'] == "cluster":
            self.builder = self.builder.master("spark://localhost:7077")
        else:
            raise ValueError("Mode non valido. Usa 'local' o 'cluster'.")

        if 'packages' in config:
            self.builder = self.builder.config("spark.jars.packages", config['packages'])

    def get_or_create(self):
        return self.builder.getOrCreate()