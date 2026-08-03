import argparse
from dotenv import load_dotenv

from .batch.Extract import PerformExtract
from .batch.Load import PerformLoad
from datetime import datetime


load_dotenv()


def main(args):
    init_date = datetime.now().strftime("%Y%m%d")
    if args.action == "batch":

        if args.operation == 'extract' and args.layer == 'source':
            obj = PerformExtract(args.layer, args.model, args.entity, init_date, args.action,)
            extracted_df = obj.extract_from_source()
            PerformLoad.LoadBronze(extracted_df, args, init_date)
            return

        elif args.operation == 'transform' and args.layer == 'silver':
            obj = PerformExtract(args.layer, args.model, args.entity, init_date, args.action, 'bronze')
            obj.extract_from_bronze()
            return

        elif args.operation == 'transform' and args.layer == 'gold':
            obj = PerformExtract(args.layer, args.model, args.entity, init_date, args.action, 'silver')
            obj.extract_from_silver()
            return

    elif args.action == "stream":

        if args.operation == 'extract' and args.layer == 'source':
            from .streaming.Extract import PerformStreamExtract
            obj = PerformStreamExtract(args.layer, args.model, args.entity, init_date, args.action)
            query = obj.extract_from_source()
            query.awaitTermination()
        return
    else:
         return




if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--action", required=True, choices=["batch", "stream"], help="Action to execute")
    parser.add_argument("--operation", required=True, choices=["extract", "transform"], help="Operation to execute")
    parser.add_argument("--layer", required=True, choices=["source", "silver", "gold"], help="ETL layer to execute")
    parser.add_argument("--model", required=True, choices=["dim", "fact"], help="ETL data modeling")
    parser.add_argument("--entity", required=True, help="Enter your entity perform ETL")
    parser.add_argument("--surrogate_key", required=False, help="Date Surrogate Key for batch processing (e.g., 20240101)") # For retry mechanism

    args = parser.parse_args()

    main(args)