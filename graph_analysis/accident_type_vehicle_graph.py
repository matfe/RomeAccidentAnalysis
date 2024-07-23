from pyspark.sql.functions import col, lit, count

from graphframes import GraphFrame

class AccidentTypeVehicleGraph:
    """
   Classe per creare un grafo che rappresenta i tipi di veicoli e i relativi incidenti."
    """
    def __init__(self, df):
        """
        Inizializza una istanza di AccidentVehiclesGraph

        :param df: DataFrame da elaboare
        """
        self.df = df
        self.df = self.df.na.drop(subset=["Protocollo", "TipoVeicolo"])
        self.df = self.df.select("Protocollo", "TipoVeicolo").distinct()

    def graph(self):
        return GraphFrame(self.create_vertices(), self.create_edges()), "AccidentTypeVehicleGraph"

    def create_vertices(self):
        """
        Crea i vertici che rappresentano tipi di veicoli e incidenti nei quali sono coinvolti.

        :return: DataFrame con i vertici del grafo
        """
        accidents_vertices = self.df.select("Protocollo") \
            .withColumnRenamed("Protocollo", "id") \
            .withColumn("type", lit("incidente")) \
            .distinct()

        vehicles_vertices = self.df.select(col("TipoVeicolo")) \
            .withColumnRenamed("TipoVeicolo", "id") \
            .withColumn("type", lit("tipo_veicolo")) \
            .distinct()

        return accidents_vertices.union(vehicles_vertices).distinct()

    def create_edges(self):
        """
        Crea gli archi tra gli incidenti e tipi di veicoli.

        :return: DataFrame con gli archi del grafo
        """
        return self.df.withColumnRenamed("Protocollo", "src") \
            .withColumnRenamed("TipoVeicolo", "dst") \
            .groupBy("src", "dst") \
            .agg(count("*").alias("weight"))