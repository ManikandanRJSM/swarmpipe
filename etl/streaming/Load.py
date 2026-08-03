import os
from dotenv import load_dotenv
from delta.tables import DeltaTable

load_dotenv()


class StreamLoad:

    @staticmethod
    def _merge_by_keys(new_df, data_path, keys):
        spark_session = new_df.sparkSession
        # Delta MERGE fails if a micro-batch has >1 source row per key; keep one.
        deduped_df = new_df.dropDuplicates(keys)

        if DeltaTable.isDeltaTable(spark_session, data_path):
            target = DeltaTable.forPath(spark_session, data_path)
            merge_condition = ' AND '.join([f"t.{key} = s.{key}" for key in keys])
            target.alias('t').merge(deduped_df.alias('s'), merge_condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            deduped_df.write.format("delta").mode("overwrite").save(data_path)

        return spark_session.read.format("delta").load(data_path)

    @staticmethod
    def upsert_silver_current(instances, clean_df, keys):
        data_lake_path = os.getenv('DATA_LAKE_PATH')
        data_path = f"{data_lake_path}/silver/{instances.model}_{instances.entity}/load_date={instances.init_date}"

        merged_df = StreamLoad._merge_by_keys(clean_df, data_path, keys)
        print(f'Streaming silver upsert done for {instances.entity}')
        return merged_df

    @staticmethod
    def upsert_gold_current(instances, agg_df, keys, entity):
        data_lake_path = os.getenv('DATA_LAKE_PATH')
        data_path = f"{data_lake_path}/gold/{instances.model}_{entity}/load_date={instances.init_date}"

        StreamLoad._merge_by_keys(agg_df, data_path, keys)
        print(f'Streaming gold upsert done for {entity}')
        return

    @staticmethod
    def upsert_silver_partition(instances, day_df, load_date, key):
        data_lake_path = os.getenv('DATA_LAKE_PATH')
        data_path = f"{data_lake_path}/silver/{instances.model}_{instances.entity}/load_date={load_date}"

        merged_df = StreamLoad._merge_by_keys(day_df, data_path, [key])
        print(f'Streaming silver upsert done for {instances.entity} load_date={load_date}')
        return merged_df

    @staticmethod
    def upsert_gold_partition(instances, agg_df, load_date, entity, keys):
        data_lake_path = os.getenv('DATA_LAKE_PATH')
        data_path = f"{data_lake_path}/gold/{instances.model}_{entity}/load_date={load_date}"

        StreamLoad._merge_by_keys(agg_df, data_path, keys)
        print(f'Streaming gold upsert done for {entity} load_date={load_date}')
        return

    @staticmethod
    def upsert_quarantine(instances, quarantine_df):
        data_lake_path = os.getenv('DATA_LAKE_PATH')
        data_path = f"{data_lake_path}/quarantine/{instances.model}_{instances.entity}/load_date={instances.init_date}"

        if DeltaTable.isDeltaTable(quarantine_df.sparkSession, data_path):
            quarantine_df.write.format("delta").mode("append").save(data_path)
        else:
            quarantine_df.write.format("delta").mode("overwrite").save(data_path)

        print(f'Streaming quarantine load done for {instances.entity}')
        return
