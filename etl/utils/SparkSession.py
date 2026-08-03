from typing import Any
from pyspark.sql import SparkSession, DataFrame

class SparkSessionFactory:
    @staticmethod
    def create_spark_session():

        #local[2] -> 2 cores if * use all cores in machine

        spark = SparkSession.builder \
                .appName("MyApp") \
                .master("local[1]") \
                .config("spark.sql.shuffle.partitions", "2") \
                .config("spark.executor.memory", "2g") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,io.delta:delta-spark_2.13:4.1.0") \
                .getOrCreate()
        # print(spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
        return spark

    @staticmethod
    def init_extraction(layer : str, data_path :str, action : str) -> DataFrame:
        spark_session = SparkSessionFactory.create_spark_session()
        if action == 'stream':
            df = spark_session.read.format("delta").load(f"{data_path}")
        else:
            df = spark_session.read.option("multiLine", "true") \
                .parquet(f"{data_path}")
        return df

    @staticmethod
    def read_batch_topic_messages(topic : str, **kwargs : Any) -> DataFrame:
        spark_session = SparkSessionFactory.create_spark_session()
        df = spark_session.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", f"{topic}") \
            .option("startingOffsets", "earliest") \
            .load()

        return df

    @staticmethod
    def read_stream_topic_messages(topic : str, **kwargs : Any) -> DataFrame:
        spark_session = SparkSessionFactory.create_spark_session()
        df = spark_session.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", f"{topic}") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        return df