import json
import os


class ConfigLoader:
    """
    Classe per caricare configurazioni da file JSON.
    """
    def __init__(self, config_path):
        self.config_path = config_path

    def load_config(self):
        """
        Carica un file di configurazione JSON e ritorna il suo contenuto come un dizionario

        :return: dizionario contenente i dati deserializzati dal file JSON
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"File non trovato in {config_path}")

        with open(self.config_path, 'r') as config_file:
            return json.load(config_file)