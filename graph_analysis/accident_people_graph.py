from pyspark.sql.functions import col, lit, count

from graphframes import GraphFrame

class AccidentPeopleGraph:
    """
    Classe per creare un grafo di persone coinvolte in incidenti.
    """
    def __init__(self, df):
        """
        Inizializza un istanza di AccidentPeopleGraph

        :param df: DataFrame da elaboare
        """
        self.df = df
        # rimuovi record con Procollo e TipoPersona nulli
        self.df = self.df.na.drop(subset=["Protocollo", "TipoPersona"])

    def graph(self):
        return GraphFrame(self.create_vertices(), self.create_edges()), "AccidentPeopleGraph"

    def create_vertices(self):
        """
        Crea i vertici per gli incidenti e le persone coinvolte.

        :return: DataFrame con i vertici del grafo
        """
        accidents_vertices = self.df.select("Protocollo") \
            .withColumnRenamed("Protocollo", "id") \
            .withColumn("type", lit("accident")) \
            .distinct()

        people_vertices = self.df.select(col("TipoPersona")) \
            .withColumnRenamed("TipoPersona", "id") \
            .withColumn("type", lit("person")) \
            .distinct()

        return accidents_vertices.union(people_vertices)

    def create_edges(self):
        """
        Crea gli archi tra incidenti e persone coinvolte.

        :return: DataFrame con gli archi del grafo
        """
        return self.df.withColumnRenamed("Protocollo", "src") \
            .withColumnRenamed("TipoPersona", "dst") \
            .groupBy("src", "dst") \
            .agg(count("*").alias("weight"))