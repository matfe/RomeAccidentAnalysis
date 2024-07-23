from pyspark.sql.functions import col, year, month, hour, count, sum, desc, max

import pandas as pd
import plotly.express as px

from util import AnalysisOutputSaver


class AccidentGeoAnalysis:
    """
    Questa classe è responsabile dell'analisi geografica degli incidenti.
    Fornisce metodi per visualizzare dati relativi agli incidenti su mappe geografiche.
    """
    def plot_maps(self, df):
        """
        Genera e visualizza mappe geografiche basate sui dati forniti.

        :param df: Il DataFrame contenente colonne con i dati geografici.
        :return: None. Il metodo visualizza direttamente le mappe.
        """
        top_zones, top_zones_conditions = self.top_zones(df)

        self.plot_accident_trends(top_zones)
        self.plot_accidents_by_nature(top_zones_conditions)

    def top_zones(self, df, limit=10000):
        """
        Identifica le zone con il maggior numero di incidenti

         :param df: Il DataFrame da elaborare.
        :param limit: Il numero massimo di zone da restituire.
        :return: DataFrame filtrato per includere solo le zone più critiche
        """
        df = df.withColumn('Anno', year(df.DataOraIncidente)) \
            .withColumn('Mese', month(df.DataOraIncidente)) \
            .withColumn('Ora', hour(df.DataOraIncidente))

        # Filtra i dati con i valori longitudine e latitudine non nulli
        filtered_df = df.filter(df["Longitudine"].isNotNull() & df["Latitudine"].isNotNull())

        # Raggruppa i dati per rimuovere i duplicati
        grouped_df = filtered_df.groupBy("Protocollo", "Longitudine", "Latitudine","Strada1", "NaturaIncidente",
                                         "CondizioneAtmosferica","ParticolaritaStrade","TipoStrada", "FondoStradale", "Traffico",
                                         "Segnaletica","Pavimentazione", "Anno", "Mese", "Ora").agg(
            count("*").alias("Totale_Incidenti"),
            max("NumFeriti").alias("Totale_Feriti"),
            max("NumMorti").alias("Totale_Morti"),
        )

        top_zones = grouped_df.groupBy("Longitudine", "Latitudine", "Anno").agg(
            count("*").alias("Totale_Incidenti"),
            sum("Totale_Feriti"),
            sum("Totale_Morti")
        ).orderBy(desc("Totale_Incidenti")).limit(limit)

        top_zones_conditions = grouped_df.groupBy("Longitudine", "Latitudine", "NaturaIncidente").agg(
            count("*").alias("Totale_Incidenti"),
            sum("Totale_Feriti"),
            sum("Totale_Morti")
        ).orderBy(desc("Totale_Incidenti")).limit(limit)

        return top_zones, top_zones_conditions

    def plot_accident_trends(self, df):
        """
        Visualizza la distribuzione degli incidenti nelle zone più critiche di Roma nel tempo.

        :param df: DataFrame contenente dati sugli incidenti.
        :return: None. Il metodo visualizza direttamente una mappa.
        """
        pandas_df = df.toPandas()

        # Ordinare i dati per anno in modo crescente
        pandas_df = pandas_df.sort_values(by='Anno')

        fig = px.density_mapbox(
            pandas_df,
            lat='Latitudine',
            lon='Longitudine',
            z='Totale_Incidenti',
            radius=10,
            center=dict(lat=41.9028, lon=12.4964),
            zoom=10,
            mapbox_style="carto-positron",
            animation_frame='Anno',
            title="Distribuzione degli incidenti nelle zone più critiche di Roma nel tempo"
        )

        fig.show()
        AnalysisOutputSaver().save_fig_html(fig, "accident_trends")

    def plot_accidents_by_nature(self, top_zones_df):
        """
        Visualizza la distribuzione degli incidenti stradali in base alla natura degli incidenti nelle zone più critiche di Roma.

        :param top_zones_df: DataFrame contenente dati sugli incidenti.
        :return: None. Il metodo visualizza direttamente una mappa.
        """
        pandas_df = top_zones_df.toPandas()

        # Creare la mappa con Plotly Express
        fig = px.scatter_mapbox(
            pandas_df,
            lat="Latitudine",
            lon="Longitudine",
            size="Totale_Incidenti",
            color="NaturaIncidente",
            hover_data=["Totale_Incidenti"],
            size_max=15,
            zoom=10,
            mapbox_style="carto-positron",
            title="Distribuzione degli incidenti stradali in base alla natura degli incidenti"
        )

        fig.show()
        AnalysisOutputSaver().save_fig_html(fig, "accidents_by_nature_map")









