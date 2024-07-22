from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from ..squirrel import SQUIRREL_CSV_HEADERS

SQUIRREL_SCHEMA = StructType([
    StructField(SQUIRREL_CSV_HEADERS["park_id"], FloatType(), True),
    StructField(SQUIRREL_CSV_HEADERS["squirrel_id"], StringType(), True),
    StructField(SQUIRREL_CSV_HEADERS["location"], StringType(), True),
    StructField(SQUIRREL_CSV_HEADERS["above_ground"], StringType(), True),
    StructField(SQUIRREL_CSV_HEADERS["specific_location"], StringType(), True),
    StructField(SQUIRREL_CSV_HEADERS["interactions_with_humans"], StringType(), True),
    StructField(SQUIRREL_CSV_HEADERS["observations"], StringType(), True),
    StructField(SQUIRREL_CSV_HEADERS["latitude"], StringType(), True),
    StructField(SQUIRREL_CSV_HEADERS["longitude"], StringType(), True),
])

SQUIRREL_ACTIVITY_SCHEMA = StructType([
    StructField(SQUIRREL_CSV_HEADERS["squirrel_id"], StringType(), True),
    StructField(SQUIRREL_CSV_HEADERS["activities"], StringType(), True)
])

SQUIRREL_FUR_COLOR_SCHEMA = StructType([
    StructField(SQUIRREL_CSV_HEADERS["park_id"], FloatType(), True),
    StructField(SQUIRREL_CSV_HEADERS["squirrel_id"], StringType(), True),
    StructField(SQUIRREL_CSV_HEADERS["primary_fur_color"], IntegerType(), True),
    StructField(SQUIRREL_CSV_HEADERS["highlights_in_fur_color"], IntegerType(), True),
    StructField(SQUIRREL_CSV_HEADERS["color_notes"], StringType(), True)
])
