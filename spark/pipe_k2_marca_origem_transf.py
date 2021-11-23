import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from spark.utils.base_functions import get_data



def minha_funcao_transformarcao(dfSrc, dfCrp):
    df_marca_origem_k2 = get_data(dfSrc)

    df_marca_origem_destino_corporate = get_data(dfCrp)

    df_transformado2 = df_marca_origem_k2.na.drop() # retirando os nulos

    df_marca_origem_destino_corporate2 = df_marca_origem_destino_corporate.na.drop() # retirando os nulos



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark da Amandinha"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("pipe_k2_marca_origem_transf")\
        .getOrCreate()

    minha_funcao_transformarcao(spark, args.src, args.dest, args.process_date)
