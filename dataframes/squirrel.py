from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, col, array, split, trim, lower, regexp_replace
from schema import SQUIRREL_CSV_HEADERS


def get_colors_df(squirrel_data: DataFrame) -> DataFrame:
    colors_df = (
        squirrel_data.select(
            explode(
                array(
                    col(SQUIRREL_CSV_HEADERS["primary_fur_color"]),
                    col(SQUIRREL_CSV_HEADERS["highlights_in_fur_color"]),
                )
            ).alias("color")
        )
        .dropna()
        .withColumn("color", explode(split(col("color"), ",")))
    )
    
    return colors_df.select(trim(lower(col("color"))).alias("color")).dropDuplicates()


def get_squirrel_df(squirrel_data: DataFrame) -> DataFrame:
    return squirrel_data.select(
        [
            SQUIRREL_CSV_HEADERS["park_id"],
            SQUIRREL_CSV_HEADERS["squirrel_id"],
            SQUIRREL_CSV_HEADERS["location"],
            SQUIRREL_CSV_HEADERS["above_ground"],
            SQUIRREL_CSV_HEADERS["specific_location"],
            SQUIRREL_CSV_HEADERS["interactions_with_humans"],
            SQUIRREL_CSV_HEADERS["observations"],
            SQUIRREL_CSV_HEADERS["latitude"],
            SQUIRREL_CSV_HEADERS["longitude"],
        ]
    )

def get_squirrel_activity_df(squirrel_data: DataFrame) -> DataFrame:
    animal_sighling = (
        squirrel_data.select([SQUIRREL_CSV_HEADERS["squirrel_id"], SQUIRREL_CSV_HEADERS["activities"]])
        .withColumn(
            "activities",
            regexp_replace("activities", r"\(.*?\)", ""),
        )
        .withColumn(
            "activities", explode(split(col("activities"), ","))
        )
    )
    
    return animal_sighling.select([
        SQUIRREL_CSV_HEADERS["squirrel_id"],
        trim(lower(col("activities"))).alias("activities")
    ]).dropDuplicates()