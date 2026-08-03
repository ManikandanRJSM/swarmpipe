from pyspark.sql import functions as F

from ..constants.constants import merge_keys
from ..helpers.calculation_helper import CalculationHelper
from .Load import StreamLoad


class StreamTransformations:

    @staticmethod
    def upsert(instances, batch_df, batch_id):

        dedup_df = batch_df.dropDuplicates()
        clean_df = dedup_df.dropna(how='any')
        quarantine_df = dedup_df.exceptAll(clean_df)

        if not quarantine_df.isEmpty():
            StreamLoad.upsert_quarantine(instances, quarantine_df)

        if clean_df.isEmpty():
            print(f'No clean records in micro-batch {batch_id} for {instances.entity}')
            return

        if instances.model == 'dim':
            StreamTransformations._upsert_dim(instances, clean_df)
        else:
            StreamTransformations._upsert_fact(instances, clean_df)

        print(f'Streaming upsert done for batch {batch_id} - {instances.entity}')
        return

    @staticmethod
    def _upsert_dim(instances, clean_df):
        keys = merge_keys[instances.entity]

        merged_silver_df = StreamLoad.upsert_silver_current(instances, clean_df, keys)
        StreamLoad.upsert_gold_current(instances, merged_silver_df, keys, entity=instances.entity)
        return

    @staticmethod
    def _upsert_fact(instances, clean_df):

        if instances.entity == 'iot_telemetry':
            df = CalculationHelper.cast_iot_columns(clean_df)
        else:
            df = clean_df.withColumn('timestamp', F.to_timestamp('timestamp'))

        dated_df = df.withColumn('load_date', F.date_format('timestamp', 'yyyyMMdd'))
        load_dates = [row['load_date'] for row in dated_df.select('load_date').distinct().collect()]

        for load_date in load_dates:
            day_df = dated_df.filter(F.col('load_date') == load_date).drop('load_date')

            merged_silver_df = StreamLoad.upsert_silver_partition(instances, day_df, load_date, key='id')

            if instances.entity == 'iot_telemetry':
                StreamLoad.upsert_gold_partition(
                    instances, CalculationHelper.hourly_energy(merged_silver_df), load_date,
                    entity='hourly_energy', keys=['site_id', 'building_id', 'asset_id', 'hour']
                )
                StreamLoad.upsert_gold_partition(
                    instances, CalculationHelper.hourly_environment(merged_silver_df), load_date,
                    entity='hourly_environment', keys=['site_id', 'building_id', 'asset_id', 'hour']
                )
                StreamLoad.upsert_gold_partition(
                    instances, CalculationHelper.daily_utilization(merged_silver_df), load_date,
                    entity='daily_utilization', keys=['site_id', 'building_id', 'asset_id', 'day']
                )
            else:
                StreamLoad.upsert_gold_partition(
                    instances, CalculationHelper.daily_faults(merged_silver_df), load_date,
                    entity='daily_faults', keys=['asset_id', 'day']
                )

        return
