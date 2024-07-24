import unittest
from pyspark.sql.functions import col
from schema import PARK_CSV_HEADERS, SQUIRREL_CSV_HEADERS
from dataframes import get_area_dt, get_park_dt, get_animal_sighling_df, get_squirrel_activity_df
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)

from spark_init import spark


class TestDataframes(unittest.TestCase):

    def test_get_area_dt(self):
        test_schema = StructType(
            [
                StructField(PARK_CSV_HEADERS["park_name"], StringType(), True),
                StructField(PARK_CSV_HEADERS["park_id"], IntegerType(), True),
                StructField(PARK_CSV_HEADERS["area_id"], StringType(), True),
                StructField(PARK_CSV_HEADERS["area_name"], StringType(), True),
            ]
        )
        data = [
            ("Fort Tryon Park", 1, "A", "MANHATTAN"),
            ("Washington Square Park", 10, "B", "CENTRAL MANHATTAN"),
        ]
        df = spark.createDataFrame(data, schema=test_schema)

        area_df = get_area_dt(df)
        self.assertIn(PARK_CSV_HEADERS["area_id"], area_df.columns)
        self.assertIn(PARK_CSV_HEADERS["area_name"], area_df.columns)
        self.assertNotIn(PARK_CSV_HEADERS["park_id"], area_df.columns)
        self.assertNotIn(PARK_CSV_HEADERS["park_name"], area_df.columns)
        self.assertEqual(len(area_df.columns), 2)

    def test_get_park_dt(self):
        test_schema = StructType(
            [
                StructField(PARK_CSV_HEADERS["park_name"], StringType(), True),
                StructField(PARK_CSV_HEADERS["park_id"], IntegerType(), True),
                StructField(PARK_CSV_HEADERS["area_id"], StringType(), True),
                StructField(PARK_CSV_HEADERS["area_name"], StringType(), True),
                StructField(PARK_CSV_HEADERS["date"], StringType(), True),
                StructField(PARK_CSV_HEADERS["start_time"], StringType(), True),
                StructField(PARK_CSV_HEADERS["end_time"], StringType(), True),
                StructField(PARK_CSV_HEADERS["total_time"], StringType(), True),
                StructField(PARK_CSV_HEADERS["park_conditions"], StringType(), True),
                StructField(
                    PARK_CSV_HEADERS["other_animal_sightings"], StringType(), True
                ),
                StructField(PARK_CSV_HEADERS["litter"], StringType(), True),
                StructField(
                    PARK_CSV_HEADERS["temperature_weather"], StringType(), True
                ),
            ]
        )
        data = [
            (
                "Fort Tryon Park",
                1,
                "A",
                "MANHATTAN",
                "3/1/20",
                "3:14:00 PM",
                "4:05:00 PM",
                "51",
                "Busy",
                "Humans, Dogs, Pigeons, Cardinals",
                "Some",
                "43 degrees, sunny",
            ),
            (
                "Fort Tryon Park",
                2,
                "B",
                "MANHATTAN",
                "3/1/20",
                "3:14:00 PM",
                "4:05:00 PM",
                "51",
                "Busy",
                "Humans, Dogs, Pigeons, Cardinals",
                "Some",
                "43 degrees, sunny",
            ),
        ]
        df = spark.createDataFrame(data, schema=test_schema)
        
        park_df = get_park_dt(df)
        
        self.assertIn(PARK_CSV_HEADERS["area_id"], park_df.columns)
        self.assertIn(PARK_CSV_HEADERS["park_name"], park_df.columns)
        self.assertIn(PARK_CSV_HEADERS["park_id"], park_df.columns)
        self.assertIn(PARK_CSV_HEADERS["date"], park_df.columns)
        self.assertIn(PARK_CSV_HEADERS["start_time"], park_df.columns)
        self.assertIn(PARK_CSV_HEADERS["end_time"], park_df.columns)
        self.assertIn(PARK_CSV_HEADERS["total_time"], park_df.columns)
        self.assertIn(PARK_CSV_HEADERS["park_conditions"], park_df.columns)
        self.assertIn(PARK_CSV_HEADERS["other_animal_sightings"], park_df.columns)
        self.assertIn(PARK_CSV_HEADERS["litter"], park_df.columns)
        self.assertIn(PARK_CSV_HEADERS["temperature_weather"], park_df.columns)

        self.assertNotIn(PARK_CSV_HEADERS["area_name"], park_df.columns)
        self.assertEqual(len(park_df.columns), 11)
        
    def test_get_animal_sighling_df(self):
        test_schema = StructType(
            [
                StructField(PARK_CSV_HEADERS["park_name"], StringType(), True),
                StructField(PARK_CSV_HEADERS["park_id"], IntegerType(), True),
                StructField(PARK_CSV_HEADERS["other_animal_sightings"], StringType(), True),
            ]
        )
        data = [
            ("Fort Tryon Park", 1, "Humans, Dogs, Pigeons, Cardinals"),
            ("Fort Tryon Park 2", 2, "Humans, Dogs, Cardinals"),
        ]
        df = spark.createDataFrame(data, schema=test_schema)

        animal_sighling_df = get_animal_sighling_df(df)
        filtered_first_park_df = animal_sighling_df.where(col("park_id") == 1)
        filtered_second_park_df = animal_sighling_df.where(col("park_id") == 2)
        
        self.assertEqual(filtered_first_park_df.count(), 4)
        self.assertEqual(filtered_second_park_df.count(), 3)
        
        self.assertIn(PARK_CSV_HEADERS["park_id"], animal_sighling_df.columns)
        self.assertIn(PARK_CSV_HEADERS["other_animal_sightings"], animal_sighling_df.columns)
        self.assertNotIn(PARK_CSV_HEADERS["park_name"], animal_sighling_df.columns)
        
    def test_get_squirrel_activity_df(self):
        test_schema = StructType(
            [
                StructField(SQUIRREL_CSV_HEADERS["squirrel_id"], StringType(), True),
                StructField(SQUIRREL_CSV_HEADERS["activities"], StringType(), True),
                StructField(SQUIRREL_CSV_HEADERS["location"], StringType(), True),
            ]
        )
        data = [
            ("A-01-01", "Eating, Digging something", "Ground Plane"),
            ("A-01-02", "Eating", "Ground Plane"),
        ]
        df = spark.createDataFrame(data, schema=test_schema)
        
        squirrel_activity_df = get_squirrel_activity_df(df)
        
        self.assertEqual(squirrel_activity_df.count(), 3)
        self.assertIn(SQUIRREL_CSV_HEADERS["squirrel_id"], squirrel_activity_df.columns)
        self.assertIn(SQUIRREL_CSV_HEADERS["activities"], squirrel_activity_df.columns)
        self.assertNotIn(SQUIRREL_CSV_HEADERS["location"], squirrel_activity_df.columns)
        

if __name__ == "__main__":
    unittest.main()
