import json

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from .config_loader import ConfigLoader

class SchemaBuilder:
    """
    Classe per costruire lo schema configurato in file JSON
    """
    def build_schema(self,  schema_config_path = "./config/schema_config.json"):
        schema_config = ConfigLoader(schema_config_path).load_config()
        fields = []
        for field in schema_config['fields']:
            data_type = self.get_field_type(field)
            field = StructField(field['name'], data_type, nullable=field['nullable'])
            fields.append(field)
        return StructType(fields)

    def get_field_type(self, field):
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
