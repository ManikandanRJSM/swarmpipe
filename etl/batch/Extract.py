import os
from dotenv import load_dotenv
from ..constants.constants import topic_prefix, entity_mapper, entity_schema
from .Transform import Transformations
from datetime import datetime

from ..utils.SparkSession import SparkSessionFactory
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import StructType, StructField, StringType

load_dotenv()

class PerformExtract:
    def __init__(self, layer, model, entity, init_date, action, extract_from = None):
        self.layer = layer
        self.model = model
        self.entity = entity
        self.extract_from = extract_from
        self.data_lake_path = os.getenv('DATA_LAKE_PATH')
        self.app_env = os.getenv("APP_ENV")
        self.init_date = init_date
        self.action = action

    def extract_from_source(self):

        topic = f"{topic_prefix}.{self.entity}"

        topic_messages = SparkSessionFactory.read_batch_topic_messages(topic, layer = self.layer, model = self.model, entity = self.entity)

        payload = topic_messages.select(col("value").cast("string").alias("value"))
        after_schema = StructType([StructField(field, StringType(), True) for field in entity_schema[self.entity]])

        parsed_df = payload.select(
            from_json(get_json_object(col("value"), "$.after"), after_schema).alias("after")
        ).select("after.*")

        print(f'Extraction done for {self.layer}')
        return parsed_df

    def extract_from_bronze(self):

        data_path = f"{self.data_lake_path}/bronze/{self.model}_{self.entity}/load_date={self.init_date}"
        bronze_df = SparkSessionFactory.init_extraction('bronze', data_path, self.action)
        print(f'Extraction done for {self.layer}')
        Transformations.dedup_null_check(self, bronze_df)
        return

    def extract_from_silver(self):

        data_path = f"{self.data_lake_path}/silver/{self.model}_{self.entity}/load_date={self.init_date}"
        silver_df = SparkSessionFactory.init_extraction('silver', data_path, self.action)
        print(f'Extraction done for {self.layer}')

        if self.model == 'dim' and self.entity in entity_mapper.get(self.model, set()):
            from .Load import PerformLoad
            PerformLoad.LoadGold(self, silver_df)

        elif self.model == 'fact':
            Transformations.gold_transformation(self, silver_df)
        else:
            return

        
        
        
        
        return