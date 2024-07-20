import os

import matplotlib.pyplot as plt
import seaborn as sns

class AnalysisOutputSaver:
    """
    Classe per salvare i risultati delle analisi in diversi formati di output nella directory specificata.
    """
    def __init__(self, output_path="output"):
        """
         Inizializza una nuova istanza della classe AnalysisOutputSaver con il percorso di una directory in cui salvare i file.

        :param output_path: directory in cui salvare i risultati delle analisi
        """
        os.makedirs(output_path, exist_ok=True)
        self.output_path = output_path
        self.html_css = "<style>body{font-family:sans-serif;margin:20px}table{width:100%;border-collapse:collapse}th,td{padding:8px;border:1px solid #ccc}</style>"
    def save_plot(self, plt, filename: str):
        """
        Salva un Plot come figura nella directory specificata.

        :param plt: matplotlib plot
        :param filename: nome della figura da salvare
        :return: None
        """
        file_path = os.path.join(self.output_path, filename)
        plt.tight_layout()
        plt.savefig(file_path, format='png')
        plt.close()

    def save_html(self, dataframe, filename: str):
        """
        Salva un DataFrame in formato HTML nella directory specificata.

        :param dataframe: DataFrame da salvare.
        :param filename: nome del file.
        :return: None
        """
        file_path = os.path.join(self.output_path, filename + '.html')
        html_with_css = self.html_css + dataframe.toPandas().to_html(index=False)
        with open(file_path, 'w') as file:
            file.write(html_with_css)

    def save_fig_html(self, fig, filename: str):
        """
        Salva una figura Plot in formato HTML nella directory specificata.

        :param fig: Figura Plot da salvare.
        :param filename: Nome del file.
        """
        file_path = os.path.join(self.output_path, filename + '.html')
        fig.write_html(file_path)

    def create_plot(self, df, x, y, kind='line', hue=None, title=None, xlabel=None, ylabel=None, rotation=0, filename=None, figsize=(16, 10)):
        """
        Crea e salva un plot basato su un DataFrame.

        :param df: DataFrame per creare il plot.
        :param x: Colonna per l'asse X.
        :param y: Colonna per l'asse Y.
        :param kind: Tipo di plot (linea o barra).
        :param hue: Colonna per la codifica del colore.
        :param title: Titolo del plot.
        :param xlabel: Etichetta dell'asse X.
        :param ylabel: Etichetta dell'asse Y.
        :param rotation: Rotazione delle etichette dell'asse X.
        :param filename: Nome del file per salvare il plot.
        :param figsize: Dimensione della figura.
        :return: None
        """
        plt.figure(figsize=figsize)

        if kind == 'line':
            sns.lineplot(x=x, y=y, hue=hue, data=df)
        elif kind == 'bar':
            sns.barplot(x=x, y=y, hue=hue, data=df)

        plt.title(title)
        plt.xlabel(xlabel, labelpad=10)
        plt.ylabel(ylabel)
        plt.xticks(rotation=rotation)

        if filename:
            file_path = os.path.join(self.output_path, filename)
            plt.tight_layout()
            plt.savefig(file_path, format='png')
        plt.close()

    def save_plots(self, results):
        """
        Salva vari plot basati sui risultati delle analisi.

        :param results: Dizionario con i risultati delle analisi.
        :return: None
        """
        for key, result in results.items():
            pandas_df = result.toPandas()

            if key == "temporal_trends":
                self.create_plot(pandas_df,x='Mese', y='Totale_Incidenti', kind='line', hue='Anno', title="Trend temporale degli incidenti",
                            xlabel="Mese", ylabel="Totale incidenti", filename=f"{key}_1.png")
                self.create_plot(pandas_df,x='Mese', y='Totale_Morti', kind='line', hue='Anno', title="Trend temporale dei morti",
                            xlabel="Mese", ylabel="Totale morti", filename=f"{key}_2.png")
                self.create_plot(pandas_df,x='Mese', y='Totale_Feriti', kind='line', hue='Anno', title="Trend temporale dei feriti",
                            xlabel="Mese", ylabel="Totale feriti", filename=f"{key}_3.png")

            elif key == "top_roads":
                self.create_plot(pandas_df, x='Strada1', y='Totale_Morti', kind='bar',
                            title="Zone con il maggior numero di morti", xlabel="Strada", ylabel="Totale morti",
                            rotation=90, filename=f"{key}_1.png")
                self.create_plot(pandas_df, x='Strada1', y='Totale_Feriti', kind='bar',
                            title="Zone con il maggior numero di feriti", xlabel="Strada", ylabel="Totale feriti",
                            rotation=90, filename=f"{key}_2.png")

            elif key == "trend_for_top_roads":
                self.create_plot(pandas_df, x='Anno', y='Totale_Incidenti', kind='line', hue='Strada1',
                            title="Trend temporale degli incidenti per le zone più pericolose", xlabel="Anno",
                            ylabel="Totale incidenti", filename=f"{key}_incidenti.png")
                self.create_plot(pandas_df, x='Anno', y='Totale_Feriti', kind='line', hue='Strada1',
                            title="Trend temporale dei feriti per le zone più pericolose", xlabel="Anno",
                            ylabel="Totale feriti", filename=f"{key}_feriti.png")
                self.create_plot(pandas_df, x='Anno', y='Totale_Morti', kind='line', hue='Strada1',
                            title="Trend temporale dei morti per le strade più pericolose", xlabel="Anno",
                            ylabel="Totale morti", filename=f"{key}_morti.png")

            elif key in {"factors", "factors_top_roads"}:
                pandas_df_melted = pandas_df.melt(id_vars='Totale_Incidenti',
                                          value_vars=['CondizioneAtmosferica', 'FondoStradale', 'Pavimentazione'], var_name='Fattore', value_name='Valore')
                self.create_plot(pandas_df_melted, x='Valore', y='Totale_Incidenti', kind='bar', hue='Fattore',
                            title="Fattori", xlabel="Fattore", ylabel="Totale incidenti", rotation=90,
                            filename=f"{key}.png")

            elif key == "hours":
                self.create_plot(pandas_df, x='Ora', y='Totale_Incidenti', kind='line', title="Incidenti durante la giornata",
                            xlabel="Ora del Giorno", ylabel="Totale Incidenti", filename=f"{key}.png")

            elif key == "hours_top_roads":
                self.create_plot(pandas_df, x='Ora', y='Totale_Incidenti', kind='line', title="Incidenti durante la giornata nelle zone con più incidenti",
                            xlabel="Ora del Giorno", ylabel="Totale Incidenti", filename=f"{key}.png")

    def save_plot_distribution(self, df, combinations):
        """
        Salva la distribuzione dei dati per combinazioni specifiche di variabili demografiche.

        :param df: DataFrame con i dati degli incidenti.
        :param combinations: Lista di tuple con combinazioni di variabili demografiche.
        :return: None
        """
        pandas_df = df.toPandas()

        for x, y in combinations:
            g = sns.catplot(data=pandas_df, kind="count", x=x, hue=y, palette="Set2", height=6, aspect=2)
            g.set_axis_labels(x, "Totale")
            g.set_titles(f"Distribuzione di {x} per {y}")
            g.set_xticklabels(rotation=90)

            file_path = os.path.join(self.output_path, f"distribution_{x}_{y}.png")

            plt.tight_layout()
            plt.savefig(file_path, format='png')
            plt.close(g.fig)