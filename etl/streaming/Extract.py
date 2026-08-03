import os
from dotenv import load_dotenv

from ..constants.constants import topic_prefix, entity_schema
from ..utils.SparkSession import SparkSessionFactory
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import StructType, StructField, StringType

from .Transform import StreamTransformations

load_dotenv()


class PerformStreamExtract:
    def __init__(self, layer, model, entity, init_date, action):
        self.layer = layer
        self.model = model
        self.entity = entity
        self.init_date = init_date
        self.action = action
        self.data_lake_path = os.getenv('DATA_LAKE_PATH')
        self.app_env = os.getenv('APP_ENV')

    def extract_from_source(self):

        topic = f"{topic_prefix}.{self.entity}"

        stream_messages = SparkSessionFactory.read_stream_topic_messages(topic, layer=self.layer, model=self.model, entity=self.entity)

        payload = stream_messages.select(col("value").cast("string").alias("value"))
        after_schema = StructType([StructField(field, StringType(), True) for field in entity_schema[self.entity]])

        parsed_df = payload.select(
            from_json(get_json_object(col("value"), "$.after"), after_schema).alias("after")
        ).select("after.*")

        checkpoint_path = f"{self.data_lake_path}/checkpoints/{self.model}_{self.entity}"

        query = parsed_df.writeStream \
            .foreachBatch(lambda batch_df, batch_id: StreamTransformations.upsert(self, batch_df, batch_id)) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime="10 seconds") \
            .start()

        print(f'Streaming started for {self.model}_{self.entity}')
        return query
