import os

class PathBuilder:
    """"
    Gestisce e costruisce percorsi di file e directory.
    """
    def __init__(self, base_path):
        self.base_path = base_path
        self.os_path = self.get_os_path()
        self.data_path = self.get_data_path()
        self.csv_path = self.get_csv_path()

    def get_os_path(self):
        return os.path.abspath(os.path.dirname(__file__))

    def get_data_path(self):
        return os.path.join(self.os_path, self.base_path)

    def get_csv_path(self):
        return os.path.join(f'file:///{self.data_path}', '*', '*.csv')

    def get_hdfs_path(self):
        raise MethodNotImplementedError
