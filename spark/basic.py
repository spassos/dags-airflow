import argparse
from os.path import join

from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def get_data(df):
    return df.select()

def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)

def data_transform(spark, src, dest, process_date):
    df = spark.read.json(src)

    data_df = get_data(df)

    table_dest = join(dest, "{table_name}", f"process_date={process_date}")

    export_json(data_df, table_dest.format(table_name="test_data"))
