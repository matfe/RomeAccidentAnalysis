import os


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
        plt.tight_layout()  #
        plt.savefig(file_path, format='png')
        plt.close()

    def save_html(self, dataframe, filename: str):
        """
        Salva un DataFrame in formato HTML nella directory specificata.

        :param dataframe: DataFrame da salvare.
        :param filename: nome del file.
        :return: None
        """
        file_path = os.path.join(self.output_path, filename)
        html_with_css = self.html_css + dataframe.toPandas().to_html(index=False)
        with open(file_path + '.html', 'w') as file:
            file.write(html_with_css)