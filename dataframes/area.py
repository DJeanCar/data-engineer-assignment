from pyspark.sql import DataFrame
from schema import PARK_CSV_HEADERS
from pyspark.sql.functions import explode, split, col, trim, lower, regexp_replace


def get_area_dt(park_data_df: DataFrame) -> DataFrame:
    area_df = park_data_df.select(
        [PARK_CSV_HEADERS["area_id"], PARK_CSV_HEADERS["area_name"]]
    ).distinct()
    return area_df


def get_park_dt(park_data_df: DataFrame) -> DataFrame:
    park_df = park_data_df.select(
        [
            PARK_CSV_HEADERS["area_id"],
            PARK_CSV_HEADERS["park_id"],
            PARK_CSV_HEADERS["park_name"],
            PARK_CSV_HEADERS["date"],
            PARK_CSV_HEADERS["start_time"],
            PARK_CSV_HEADERS["end_time"],
            PARK_CSV_HEADERS["total_time"],
            PARK_CSV_HEADERS["park_conditions"],
            PARK_CSV_HEADERS["other_animal_sightings"],
            PARK_CSV_HEADERS["litter"],
            PARK_CSV_HEADERS["temperature_weather"],
        ]
    )
    return park_df


def get_animal_sighling_df(park_data: DataFrame) -> DataFrame:
    animal_sighling = (
        park_data.select([PARK_CSV_HEADERS["park_id"], PARK_CSV_HEADERS["other_animal_sightings"]])
        .withColumn(
            "other_animal_sightings",
            regexp_replace("other_animal_sightings", r"\(.*?\)", ""),
        )
        .withColumn(
            "other_animal_sightings", explode(split(col("other_animal_sightings"), ","))
        )
    )

    return animal_sighling.select([
        PARK_CSV_HEADERS["park_id"],
        trim(lower(col("other_animal_sightings"))).alias("other_animal_sightings")
    ]).dropDuplicates()
