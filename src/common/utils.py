
def load_csv_file(spark, csv_file):
    return (
        spark.read \
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(csv_file)
    )