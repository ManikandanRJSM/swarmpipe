from pyspark.sql import functions as F


class CalculationHelper:

    @staticmethod
    def cast_iot_columns(data_frame):
        return data_frame \
            .withColumn('timestamp', F.to_timestamp('timestamp')) \
            .withColumn('power_consumption', F.col('power_consumption').cast('double')) \
            .withColumn('temperature', F.col('temperature').cast('double')) \
            .withColumn('humidity', F.col('humidity').cast('double')) \
            .withColumn('pressure', F.col('pressure').cast('double')) \
            .withColumn('vibration', F.col('vibration').cast('double'))

    @staticmethod
    def hourly_energy(data_frame):
        return data_frame.groupBy(
            'site_id', 'building_id', 'asset_id',
            F.date_trunc('hour', 'timestamp').alias('hour')
        ).agg(F.sum('power_consumption').alias('total_power_consumption'))

    @staticmethod
    def hourly_environment(data_frame):
        return data_frame.groupBy(
            'site_id', 'building_id', 'asset_id',
            F.date_trunc('hour', 'timestamp').alias('hour')
        ).agg(
            F.avg('temperature').alias('avg_temperature'),
            F.avg('humidity').alias('avg_humidity'),
            F.avg('pressure').alias('avg_pressure'),
            F.avg('vibration').alias('avg_vibration')
        )

    @staticmethod
    def daily_utilization(data_frame):
        return data_frame.groupBy(
            'site_id', 'building_id', 'asset_id',
            F.to_date('timestamp').alias('day')
        ).agg(
            F.count(F.when(F.col('operating_mode') == 'running', True)).alias('running_readings'),
            F.count('*').alias('total_readings')
        ).withColumn(
            'utilization_pct', (F.col('running_readings') / F.col('total_readings')) * 100
        )

    @staticmethod
    def daily_faults(data_frame):
        df = data_frame.withColumn('timestamp', F.to_timestamp('timestamp'))
        return df.filter(F.col('event_type') == 'Fault') \
            .groupBy('asset_id', F.to_date('timestamp').alias('day')) \
            .agg(
                F.count('*').alias('fault_count'),
                F.sum(F.when(F.col('severity') == 'High', 1).otherwise(0)).alias('high_severity_count'),
                F.sum(F.when(F.col('severity') == 'Medium', 1).otherwise(0)).alias('medium_severity_count'),
                F.sum(F.when(F.col('severity') == 'Low', 1).otherwise(0)).alias('low_severity_count')
            )
