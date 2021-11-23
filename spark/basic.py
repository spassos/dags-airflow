import argparse
from os.path import join

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from opt.airflow.dags.repo.spark.utils.base_functions import get_data


def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)

def data_transform(spark, src, dest, process_date):
    df = spark.read.json(src)

    data_df = get_data(df)

    table_dest = join(dest, "{table_name}", f"process_date={process_date}")

    export_json(data_df, table_dest.format(table_name="test_data"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("basic")\
        .getOrCreate()

    data_transform(spark, args.src, args.dest, args.process_date)
