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

    def save_csv(self, dataframe, filename: str):
        """
        Salva un DataFrame in formato CSV nella directory specificata.

        :param dataframe: DataFrame da salvare.
        :param filename: nome del file CSV.
        :return: None
        """
        file_path = os.path.join(self.output_path, filename)
        dataframe.to_csv(file_path, index=False)
