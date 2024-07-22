from pyspark.sql.functions import col, lit, count

from graphframes import GraphFrame

class AccidentCrossroadsGraph:
    """
    Classe per creare un grafo di incidenti avvenuti agli incroci stradali.
    """
    def __init__(self, df):
        """
        Inizializza una istanza di AccidentCrossroadsGraph

        Il DataFrame viene filtrato per includere solo gli incidenti avvenuti agli incroci stradali.

        :param df: DataFrame da elaboare
        """
        self.df = df
        self.df = self.df.na.drop(subset=["Strada1", "Strada02", "Localizzazione2"])
        self.df = self.df.select("Protocollo", "Strada1", "Strada02", "Localizzazione2") \
            .filter(col("Localizzazione2").like("%all'intersezione%")).distinct()

    def graph(self):
        return GraphFrame(self.create_vertices(), self.create_edges()), "AccidentCrossroadsGraph"

    def create_vertices(self):
        """
        Crea i vertici per le strade che si intersecano.

        :return: DataFrame con i vertici del grafo
        """
        strada1_vertices = self.df.select("Strada1") \
            .withColumnRenamed("Strada1", "id") \
            .withColumn("type", lit("strada")) \
            .distinct()

        strada2_vertices = self.df.select(col("Strada02")) \
            .withColumnRenamed("Strada02", "id") \
            .withColumn("type", lit("strada")) \
            .distinct()

        return strada1_vertices.union(strada2_vertices).distinct()

    def create_edges(self):
        """
        Crea gli archi tra le strade che si intersecano.

        :return: DataFrame con gli archi del grafo
        """
        return self.df.withColumnRenamed("Strada1", "src") \
            .withColumnRenamed("Strada02", "dst") \
            .groupBy("src", "dst") \
            .agg(count("*").alias("weight"))