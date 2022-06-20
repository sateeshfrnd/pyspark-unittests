import sys

try:
    import findspark

    findspark.init()
    import pyspark
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
except ImportError as e:
    print("Error importing Spark Modules", e)
    sys.exit(1)

def load_data(spark, csv_file):
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(csv_file)
    )

def aggegrate_by_loc(df):
    return df.groupby('location').count()


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName('agg_data')
            .master("local")
            # .config("spark.submit.pyFiles", f"{spark_home}/python/lib/pyspark.zip")
            .config("spark.submit.deployMode", "client")
            .getOrCreate()
    )
    print(spark)
    df = load_data(spark, 'data/sample.csv')
    df.printSchema()
    df.show()

    agg_df = aggegrate_by_loc(df)
    agg_df.show()

