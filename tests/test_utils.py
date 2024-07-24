import unittest
from datetime import datetime, time
from utils.date import parse_str_to_date, parse_str_to_time
from utils.csv import read_csv_file
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)

class TestUtils(unittest.TestCase):

    def test_parse_str_to_date(self):
        date_str = "3/1/20"
        date_parsed = parse_str_to_date(date_str)
        self.assertIsInstance(date_parsed, datetime)
        self.assertEqual(date_parsed.day, 3)
        self.assertEqual(date_parsed.month, 1)
        self.assertEqual(date_parsed.year, 2020)
        
    def test_parse_str_to_time(self):
        time_str = "3:14:00 PM"
        time_parsed = parse_str_to_time(time_str)
        self.assertIsInstance(time_parsed, time)
        
    def test_read_csv_file(self):
        test_schema = StructType(
            [
                StructField("color_id", IntegerType(), True),
                StructField("color_name", StringType(), True),
            ]
        )
        df = read_csv_file("./tests/color_test.csv", schema=test_schema)
        self.assertIsInstance(df, DataFrame)
        self.assertIn("color_id", df.columns)
        self.assertIn("color_name", df.columns)
        
if __name__ == "__main__":
    unittest.main()
