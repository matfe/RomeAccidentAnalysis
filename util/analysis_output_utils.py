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
        os.makedirs(output_dir, exist_ok=True)
        self.output_path = output_path

    def save_plot(self, plt, filename):
        """
        Salva un Plot nella directory specificata.

        :param plt: matplotlib plot
        :param filename: nome del Plot da salvare
        :return: None
        """
        file_path = os.path.join(self.output_dir, filename)
        plt.savefig(file_path)
        plt.close()

    def save_csv(dataframe, filename):
        """
        Salva un DataFrame in formato CSV nella directory specificata.

        :param dataframe: DataFrame da salvare.
        :param filename: nome del file CSV.
        :return: None
        """
        file_path = os.path.join(directory_path, file_name)
        dataframe.to_csv(file_path, mode='overwrite', index=False)
