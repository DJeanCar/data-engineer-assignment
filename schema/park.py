from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

PARK_CSV_HEADERS = dict(
    area_name = "area_name",
    area_id = "area_id",
    park_name = "park_name",
    park_id = "park_id",
    date = "date",
    start_time = "start_time",
    end_time = "end_time",
    total_time = "total_time",
    park_conditions = "park_conditions",
    other_animal_sightings = "other_animal_sightings",
    litter = "litter",
    temperature_weather = "temperature_weather"
)

PARK_SCHEMA = StructType([
    StructField("Area Name", StringType(), True),
    StructField("Area ID", StringType(), True),
    StructField("Park Name", StringType(), True),
    StructField("Park ID", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Start Time", StringType(), True),
    StructField("End Time", StringType(), True),
    StructField("Total Time (in minutes, if available)", IntegerType(), True),
    StructField("Park Conditions", StringType(), True),
    StructField("Other Animal Sightings", StringType(), True),
    StructField("Litter", StringType(), True),
    StructField("Temperature & Weather", StringType(), True),
])

PARK_COLUMNS_RENAME_MAP = {
    "Area Name": "area_name",
    "Area ID": "area_id",
    "Park Name": "park_name",
    "Park ID": "park_id",
    "Date": "date",
    "Start Time": "start_time",
    "End Time": "end_time",
    "Total Time (in minutes, if available)": "total_time",
    "Park Conditions": "park_conditions",
    "Other Animal Sightings": "other_animal_sightings",
    "Litter": "litter",
    "Temperature & Weather": "temperature_weather"
}
