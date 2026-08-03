import os
from dotenv import load_dotenv

from ..utils.SparkSession import SparkSessionFactory

load_dotenv()


class PerformLoad:

    @staticmethod
    def LoadBronze(data_frame, reqst_args, init_date):

        data_lake_path = os.getenv('DATA_LAKE_PATH')
        data_path = f"{data_lake_path}/bronze/{reqst_args.model}_{reqst_args.entity}/load_date={init_date}"
        data_frame.write.mode("overwrite") \
        .parquet(f"{data_path}")
        return

    @staticmethod
    def LoadSilver(reqst_args, data_frame):

        init_date = reqst_args.init_date
        data_lake_path = os.getenv('DATA_LAKE_PATH')
        data_path = f"{data_lake_path}/silver/{reqst_args.model}_{reqst_args.entity}/load_date={init_date}"
        data_frame.write.mode("overwrite") \
        .parquet(f"{data_path}")
        print(f'Load done for {reqst_args.layer}')
        return

    @staticmethod
    def LoadGold(reqst_args, data_frame, entity = None):

        init_date = reqst_args.init_date
        data_lake_path = os.getenv('DATA_LAKE_PATH')
        data_path = f"{data_lake_path}/gold/{reqst_args.model}_{entity or reqst_args.entity}/load_date={init_date}"
        data_frame.write.mode("overwrite") \
        .parquet(f"{data_path}")
        print(f'Load done for {reqst_args.layer} - {entity}')
        return

    @staticmethod
    def LoadQuarantine(reqst_args, data_frame):

        init_date = reqst_args.init_date
        data_lake_path = os.getenv('DATA_LAKE_PATH')
        data_path = f"{data_lake_path}/quarantine/{reqst_args.model}_{reqst_args.entity}/load_date={init_date}"
        data_frame.write.mode("overwrite") \
        .parquet(f"{data_path}")
        print(f'Quarantine load done for {reqst_args.layer}')
        return
