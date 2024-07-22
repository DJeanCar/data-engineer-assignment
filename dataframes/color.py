from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, split, col, trim, lower
from db import postgres_db
from spark_init import spark
from schema import COLOR_SCHEMA, SQUIRREL_CSV_HEADERS


def get_fur_color_df(squirrel_data: DataFrame) -> DataFrame:
    colors_db = postgres_db.select_query("SELECT * FROM color;")
    colors_df = spark.createDataFrame(colors_db, schema=COLOR_SCHEMA)
    
    squirrel_color_df = (
        squirrel_data.select(
            [
                SQUIRREL_CSV_HEADERS["park_id"],
                SQUIRREL_CSV_HEADERS["squirrel_id"],
                SQUIRREL_CSV_HEADERS["primary_fur_color"],
                SQUIRREL_CSV_HEADERS["highlights_in_fur_color"],
                SQUIRREL_CSV_HEADERS["color_notes"]
            ]
        )
        .withColumn(
            "highlights_in_fur_color",
            explode(split(col("highlights_in_fur_color"), ",")),
        )
        .withColumn(
            "highlights_in_fur_color", trim(lower(col("highlights_in_fur_color")))
        )
        .withColumn("primary_fur_color", trim(lower(col("primary_fur_color"))))
    )

    colors_with_primary: DataFrame = (
        squirrel_color_df.join(
            colors_df,
            colors_df.name == squirrel_color_df.primary_fur_color,
            how="inner",
        )
        .select(
            [
                SQUIRREL_CSV_HEADERS["park_id"],
                SQUIRREL_CSV_HEADERS["squirrel_id"],
                col("id").alias("primary_fur_color"),
                SQUIRREL_CSV_HEADERS["highlights_in_fur_color"],
                SQUIRREL_CSV_HEADERS["color_notes"]
            ]
        )
    )
    
    return colors_with_primary.join(
            colors_df,
            colors_df.name == squirrel_color_df.highlights_in_fur_color,
            how="inner",
        ).select(
            [
                SQUIRREL_CSV_HEADERS["park_id"],
                SQUIRREL_CSV_HEADERS["squirrel_id"],
                SQUIRREL_CSV_HEADERS["primary_fur_color"],
                col("id").alias("highlights_in_fur_color"),
                SQUIRREL_CSV_HEADERS["color_notes"]
            ]
        )
