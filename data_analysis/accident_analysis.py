from pyspark.sql.functions import col, year, month, dayofmonth, hour, count, avg, corr, date_format, weekofyear, sum, max

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from util import AnalysisOutputSaver


class AccidentAnalysis:
    """
    Classe per l'analisi degli incidenti stradali.
    """

    def analyze(self, df):
        """
        Analizza un DataFrame di incidenti e restituisce vari risultati di analisi.

        :param df: DataFrame con i dati sugli incidenti
        :return: Un dizionario contenente i risultati delle analisi
        """
        df = self.prepare(df)
        df_without_duplicates = self.remove_duplicates(df)

        temporal_trends = self.analyze_temporal_trends(df_without_duplicates)
        top_roads = self.analyze_top_roads(df_without_duplicates)
        factors = self.analyze_factors(df_without_duplicates)
        factors_top_roads = self.analyze_factors(df_without_duplicates, top_roads)
        hours = self.analyze_hours(df_without_duplicates)
        hours_top_roads = self.analyze_hours(df_without_duplicates, top_roads)


        return {
            'temporal_trends': temporal_trends,
            'top_roads': top_roads,
            'trend_for_top_roads': self.analyze_trend_for_top_roads(df_without_duplicates, top_roads),
            'factors': factors,
            'factors_top_roads': factors_top_roads,
            'hours': hours,
            'hours_top_roads': hours_top_roads
        }

    def get_demographic_combinations_for_analysis(self):
        """
        Restituisce le combinazioni di aspetti demografici da analizzare

        :return: Lista di tuple con le combinazioni demografiche
        """
        # Combinazioni di aspetti demografici da analizzare
        combinations = [
            ("Sesso", "TipoLesione"),
            ("TipoVeicolo", "Sesso"),
            ("TipoLesione", "StatoVeicolo"),
            ("TipoPersona", "CinturaCascoUtilizzato")
        ]

        return combinations

    def prepare(self, df):
        """
        Prepara il DataFrame riempiendo i valori nulli e aggiungendo colonne di anno, mese e ora.

        :param df: DataFrame da elaborare
        :return: DataFrame preparato
        """
        df = df.fillna({'NumFeriti': 0, 'NumMorti': 0})
        df = df.filter(col('DataOraIncidente').isNotNull())

        df = df.withColumn('Anno', year(df.DataOraIncidente)) \
            .withColumn('Mese', month(df.DataOraIncidente)) \
            .withColumn('Ora', hour(df.DataOraIncidente))

        return df

    def remove_duplicates(self, df):
        """
        Aggrega i dati per protocollo per evitare i duplicati.

        :param df: DataFrame da elaborare
        :return: DataFrame senza duplicati
        """
        return df.groupBy("Protocollo", "Anno", "Mese", "Ora", "Strada1",  "CondizioneAtmosferica", "FondoStradale", "Pavimentazione", "Illuminazione").agg(
            max("NumFeriti").alias("NumFeriti"),
            max("NumMorti").alias("NumMorti"),
            max("NumIllesi").alias("NumIllesi"))

    def analyze_temporal_trends(self, df):
        """
        Analizza le tendenze temporali degli incidenti.

        :param df: DataFrame da elaborare
        :return: DataFrame con le tendenze temporali
        """
        # Aggrega per anno e mese
        temporal_trends = df.groupBy("Anno", "Mese").agg(
            count("*").alias("Totale_Incidenti"),
            sum("NumFeriti").alias("Totale_Feriti"),
            sum("NumMorti").alias("Totale_Morti")
        ).orderBy("Anno", "Mese")

        return temporal_trends

    def analyze_top_roads(self, df, top_n=20):
        """
        Analizza le strade con il maggior numero di incidenti.

        :param df: DataFrame da elaborare
        :param top_n: Numero di strade da includere nell'analisi (default 20)
        :return: DataFrame con le strade più pericolose
        """
        top_roads = df.groupBy(col("Strada1")).agg(
            sum(col("NumFeriti")).alias("Totale_Feriti"),
            sum(col("NumMorti")).alias("Totale_Morti"),
        ).orderBy(col("Totale_Morti").desc(), col("Totale_Feriti").desc()).limit(top_n)

        return top_roads

    def analyze_trend_for_top_roads(self, df, top_roads):
        """
        Analizza le tendenze degli incidenti per le strade più pericolose.

        :param df: DataFrame da elaborare
        :param top_roads: DataFrame con le strade più pericolose
        :return: DataFrame con le tendenze per le strade più pericolose
        """
        # Filtra il dataframe per includere solo le zone più pericolose
        top_roads_list = [row['Strada1'] for row in top_roads.collect()]
        df_filtered = df.filter(col("Strada1").isin(top_roads_list))

        # Calcola i trend temporali per le zone più pericolose
        trend_for_top_roads = df_filtered.groupBy("Anno", "Mese", "Strada1").agg(
            count("*").alias("Totale_Incidenti"),
            sum(col("NumFeriti")).alias("Totale_Feriti"),
            sum(col("NumMorti")).alias("Totale_Morti")
        ).orderBy("Strada1", "Anno", "Mese")

        return trend_for_top_roads

    def analyze_factors(self, df, roads=None):
        """
        Analizza i fattori che influenzano gli incidenti stradali.

        :param df: DataFrame da elaborare
        :param roads: Lista delle strade specifiche da analizzare (opzionale)
        :return: DataFrame con l'analisi dei fattori
        """
        if roads is not None:
            roads_list = [row['Strada1'] for row in roads.collect()]
            df = df.filter(col("Strada1").isin(roads_list))

        factors_analysis = df.groupBy("Anno", "CondizioneAtmosferica", "FondoStradale", "Pavimentazione", "Illuminazione").agg(
            count("*").alias("Totale_Incidenti"),
            sum(col("NumFeriti")).alias("Totale_Feriti"),
            sum(col("NumMorti")).alias("Totale_Morti")
        ).orderBy("Anno", "Totale_Incidenti", ascending=False)

        return factors_analysis

    def analyze_hours(self, df, roads=None):
        """
        Analizza la distribuzione oraria degli incidenti.

        :param df: DataFrame senza duplicati.
        :param roads: Lista delle strade specifiche da analizzare (opzionale)
        :return: DataFrame con l'analisi oraria.
        """
        if roads is not None:
            roads_list = [row['Strada1'] for row in roads.collect()]
            df = df.filter(col("Strada1").isin(roads_list))

        hours_analysis = df.groupBy("Ora").agg(
            count("*").alias("Totale_Incidenti"),
            sum(col("NumFeriti")).alias("Totale_Feriti"),
            sum(col("NumMorti")).alias("Totale_Morti")
        ).orderBy("Ora")

        return hours_analysis