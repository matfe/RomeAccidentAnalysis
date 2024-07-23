import json

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from util import ConfigLoader


class SchemaBuilder:
    """
    Classe per creare e gestisce lo schema configurato
    """
    def build_schema(self,  schema_config_path = "./config/schema_config.json"):
        """
         Costruisce uno schema per i dati utilizzando una configurazione specificata.

        :param schema_config_path: Il percorso del file di configurazione dello schema.
        :return: Un oggetto StructType che rappresenta lo schema costruito.
        """
        schema_config = ConfigLoader(schema_config_path).load_config()
        fields = []
        for field in schema_config['fields']:
            data_type = self.get_field_type(field)
            field = StructField(field['name'], data_type, nullable=field['nullable'])
            fields.append(field)
        return StructType(fields)

    def get_field_type(self, field):
        """
           Determina il tipo di dato per un campo specificato.

           :param field: Un dizionario che rappresenta il campo, contenente una chiave 'type' il cui valore che descrive il tipo del campo.
           :return: Un oggetto tipo che rappresenta il tipo di dato appropriato (StringType, IntegerType, DoubleType).
           """
        field_type = field['type']
        if field_type == 'string':
            data_type = StringType()
        elif field_type == 'integer':
            data_type = IntegerType()
        elif field_type == 'double':
            data_type = DoubleType()
        else:
            data_type = StringType()
        return data_type
