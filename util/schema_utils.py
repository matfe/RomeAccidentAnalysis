import json

from pyspark.sql.types import StructField,StructType,StringType
from .config_loader import ConfigLoader

class SchemaBuilder:
    """
    Classe per costruire lo schema configurato in file JSON
    """
    def build_schema(self,  schema_config_path = "./config/schema_config.json"):
        schema_config = ConfigLoader(schema_config_path).load_config()
        fields = []
        for field in schema_config['fields']:
            field = StructField(field['name'], StringType(), nullable=field['nullable'])
            fields.append(field)

        return StructType(fields)
