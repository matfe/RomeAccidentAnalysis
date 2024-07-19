"""
Il pacchetto data_preparation contiene i moduli per preparare i dati per le analisi successive.

Moduli:
    loader: carica i dati dal dataset
    cleaner: pulisce e normalizza i dati
    explorer: esplora le caratteristiche dei dati
"""

from .loader import DataLoader
from .cleaner import DataCleaner
from .explorer import DataAnalysisExplorer