from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from ..park import PARK_CSV_HEADERS

PARK_SCHEMA = StructType([
    StructField(PARK_CSV_HEADERS["area_id"], StringType(), True),
    StructField(PARK_CSV_HEADERS["park_id"], FloatType(), True),
    StructField(PARK_CSV_HEADERS["park_name"], StringType(), True),
    StructField(PARK_CSV_HEADERS["date"], StringType(), True),
    StructField(PARK_CSV_HEADERS["start_time"], StringType(), True),
    StructField(PARK_CSV_HEADERS["end_time"], StringType(), True),
    StructField(PARK_CSV_HEADERS["total_time"], StringType(), True),
    StructField(PARK_CSV_HEADERS["park_conditions"], IntegerType(), True),
    StructField(PARK_CSV_HEADERS["other_animal_sightings"], StringType(), True),
    StructField(PARK_CSV_HEADERS["litter"], StringType(), True),
    StructField(PARK_CSV_HEADERS["temperature_weather"], StringType(), True),
])

AREA_SCHEMA = StructType([
    StructField(PARK_CSV_HEADERS["area_id"], StringType(), True),
    StructField(PARK_CSV_HEADERS["area_name"], StringType(), True)
])

ANIMAL_SIGHLING_SCHEMA = StructType([
    StructField(PARK_CSV_HEADERS["park_id"], StringType(), True),
    StructField(PARK_CSV_HEADERS["other_animal_sightings"], StringType(), True)
])
