from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

SQUIRREL_CSV_HEADERS = dict(
    park_id = "park_id",
    squirrel_id = "squirrel_id",
    primary_fur_color = "primary_fur_color",
    highlights_in_fur_color = "highlights_in_fur_color",
    color_notes = "color_notes",
    location = "location",
    above_ground = "above_ground",
    specific_location = "specific_location",
    activities = "activities",
    interactions_with_humans = "interactions_with_humans",
    observations = "observations",
    latitude = "latitude",
    longitude = "longitude"
)

SQUIRREL_SCHEMA = StructType([
    StructField("Park ID", FloatType(), True),
    StructField("Squirrel ID", StringType(), True),
    StructField("Primary Fur Color", StringType(), True),
    StructField("Highlights in Fur Color", StringType(), True),
    StructField("Color Notes", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Above Ground (Height in Feet)", StringType(), True),
    StructField("Specific Location", StringType(), True),
    StructField("Activities", StringType(), True),
    StructField("Interactions with Humans", StringType(), True),
    StructField("Other Notes or Observations", StringType(), True),
    StructField("Squirrel Latitude (DD.DDDDDD)", FloatType(), True),
    StructField("Squirrel Longitude (-DD.DDDDDD)", FloatType(), True),
])

SQUIRREL_COLUMNS_RENAME_MAP = {
    "Park ID": "park_id",
    "Squirrel ID": "squirrel_id",
    "Primary Fur Color": "primary_fur_color",
    "Highlights in Fur Color": "highlights_in_fur_color",
    "Color Notes": "color_notes",
    "Location": "location",
    "Above Ground (Height in Feet)": "above_ground",
    "Specific Location": "specific_location",
    "Activities": "activities",
    "Interactions with Humans": "interactions_with_humans",
    "Other Notes or Observations": "observations",
    "Squirrel Latitude (DD.DDDDDD)": "latitude",
    "Squirrel Longitude (-DD.DDDDDD)": "longitude"
}
