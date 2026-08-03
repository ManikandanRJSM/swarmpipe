from .Load import PerformLoad
from ..helpers.calculation_helper import CalculationHelper

class Transformations:

    @staticmethod
    def dedup_null_check(instances, data_frame):
        dedup_df = data_frame.dropDuplicates()

        clean_df = dedup_df.dropna(how='any')
        quarantine_df = dedup_df.exceptAll(clean_df)

        print(f'Dedup and null check transformation done for {instances.layer}')

        PerformLoad.LoadSilver(instances, clean_df)
        PerformLoad.LoadQuarantine(instances, quarantine_df)
        return

    @staticmethod
    def gold_transformation(instances, data_frame):

        if instances.entity == 'iot_telemetry':
            df = CalculationHelper.cast_iot_columns(data_frame)

            PerformLoad.LoadGold(instances, CalculationHelper.hourly_energy(df), entity='hourly_energy')
            PerformLoad.LoadGold(instances, CalculationHelper.hourly_environment(df), entity='hourly_environment')
            PerformLoad.LoadGold(instances, CalculationHelper.daily_utilization(df), entity='daily_utilization')

        elif instances.entity == 'events':
            PerformLoad.LoadGold(instances, CalculationHelper.daily_faults(data_frame), entity='daily_faults')

        print(f'Gold transformation done for {instances.layer}')
        return