import unittest
import sys



try:
    import findspark

    findspark.init()
    import pyspark
    from pyspark.sql import DataFrame, SparkSession
except ImportError as e:
    print("Error importing Spark Modules", e)
    sys.exit(1)

from aggregate_data import load_data, aggegrate_by_loc

class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
                .master("local")
                .appName("aggregate_test")
                .getOrCreate()
        )
        cls.sample_file= file_path = 'file:/E:/Github-Repos/2022/pyspark-unittests/src/data/sample.csv'

    def test_load_data(self):
        file_path = 'file:/E:/Github-Repos/2022/pyspark-unittests/src/data/sample.csv'
        df = load_data(self.spark, self.sample_file)
        self.assertEqual(df.count(), 5, 'Record counts should be 5')

    def test_aggegrate_by_loc(self):
        df = load_data(self.spark, self.sample_file)
        locations_df = aggegrate_by_loc(df)
        locations_df.printSchema()

        locations = aggegrate_by_loc(df).collect()
        self.assertEqual(len(locations), 3, 'Number of Locations shouldbe 3')
        locations_dict = dict()
        for row in locations:
            locations_dict[row["location"]] = row["count"]
        self.assertEqual(locations_dict["MeenakshiLayout"], 2, "MeenakshiLayout record Count should be 2")
        self.assertEqual(locations_dict["Kasavanahalli"], 2, "Kasavanahalli record Count should be 2")
        self.assertEqual(locations_dict["SarjapurRoad"], 1, "SarjapurRoad record Count should be 1")

if __name__ == '__main__':
    unittest.main()
