from pyspark.sql.functions import col, lit, count

from graphframes import GraphFrame

class AccidentRoadsGraph:
    """
    Classe per creare un grafo delle strade con incidenti.
    """
    def __init__(self, df):
        """
        Inizializza una istanza di AccidentRoadsGraph

        :param df: DataFrame da elaboare
        """
        self.df = df
        self.df = self.df.na.drop(subset=["Protocollo", "Strada1"])
        self.df = self.df.select("Protocollo", "Strada1").distinct()

    def graph(self):
        return GraphFrame(self.create_vertices(), self.create_edges()), "AccidentRoadsGraph"

    def create_vertices(self):
        """
        Crea i vertici che rappresentano le strade con incidenti.

        :return: DataFrame con i vertici del grafo
        """
        accidents_vertices = self.df.select("Protocollo") \
            .withColumnRenamed("Protocollo", "id") \
            .withColumn("type", lit("incidente")) \
            .distinct()

        roads_vertices = self.df.select(col("Strada1")) \
            .withColumnRenamed("Strada1", "id") \
            .withColumn("type", lit("strada")) \
            .distinct()

        return accidents_vertices.union(roads_vertices).distinct()

    def create_edges(self):
        """
        Crea gli archi tra gli incidenti e le strade.

        :return: DataFrame con gli archi del grafo
        """
        return self.df.withColumnRenamed("Protocollo", "src") \
            .withColumnRenamed("Strada1", "dst") \
            .groupBy("src", "dst") \
            .agg(count("*").alias("weight"))