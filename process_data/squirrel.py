from pyspark.sql import DataFrame
from dataframes import (
    get_colors_df,
    get_squirrel_df,
    get_fur_color_df,
    get_squirrel_activity_df,
)
from entity.color import Color
from entity.squirrel import SquirrelBuilder
from entity.activity import Activity
from entity.fur_color import FurColor
from db import postgres_db
from pathlib import Path
from utils.csv import save_csv_file, read_csv_file
from schema.processed_files import (
    COLOR_SCHEMA,
    SQUIRREL_SCHEMA,
    SQUIRREL_ACTIVITY_SCHEMA,
    SQUIRREL_FUR_COLOR_SCHEMA,
)

TEMP_DIRECTORY = "temp"
COLOR_PROCESSED_FILENAME = "color.csv"
SQUIRREL_PROCESSED_FILENAME = "squirrel.csv"
SQUIRREL_ACTIVITY_PROCESSED_FILENAME = "squirrel_activity.csv"
SQUIRREL_FUR_COLOR_PROCESSED_FILENAME = "squirrel_fur_color.csv"


def proccess_squirrel_dataframes(squirrel_data: DataFrame, processed_folderpath: str):
    folder_path = Path(processed_folderpath)
    temp_directory = folder_path / TEMP_DIRECTORY

    color_df = get_colors_df(squirrel_data)
    squirrel_df = get_squirrel_df(squirrel_data)
    squirrel_activity_df = get_squirrel_activity_df(squirrel_data)
    fur_color_df = get_fur_color_df(squirrel_data)

    save_csv_file(color_df, temp_directory, folder_path / COLOR_PROCESSED_FILENAME)
    save_csv_file(
        squirrel_df, temp_directory, folder_path / SQUIRREL_PROCESSED_FILENAME
    )
    save_csv_file(
        squirrel_activity_df,
        temp_directory,
        folder_path / SQUIRREL_ACTIVITY_PROCESSED_FILENAME,
    )
    save_csv_file(
        fur_color_df,
        temp_directory,
        folder_path / SQUIRREL_FUR_COLOR_PROCESSED_FILENAME,
    )


def process_squirrel_data(processed_folderpath: str):
    folder_path = Path(processed_folderpath)

    color_filepath = str(folder_path / COLOR_PROCESSED_FILENAME)
    colors = [
        Color(row["color"]).to_dict()
        for row in read_csv_file(color_filepath, schema=COLOR_SCHEMA).collect()
    ]

    squirrel_filepath = str(folder_path / SQUIRREL_PROCESSED_FILENAME)
    squirrels = [
        SquirrelBuilder()
        .set_squirrel_id(row["park_id"], row["squirrel_id"])
        .set_location(
            row["location"],
            row["above_ground"],
            row["specific_location"],
            row["latitude"],
            row["longitude"],
        )
        .set_aditional_info(row["interactions_with_humans"], row["observations"])
        .build()
        .to_dict()
        for row in read_csv_file(squirrel_filepath, schema=SQUIRREL_SCHEMA).collect()
    ]

    squirrel_activity_filepath = str(folder_path / SQUIRREL_ACTIVITY_PROCESSED_FILENAME)
    squirrel_activities = [
        Activity(row["squirrel_id"], row["activities"]).to_dict()
        for row in read_csv_file(
            squirrel_activity_filepath, schema=SQUIRREL_ACTIVITY_SCHEMA
        ).collect()
    ]

    squirrel_fur_color_filepath = str(
        folder_path / SQUIRREL_FUR_COLOR_PROCESSED_FILENAME
    )
    fur_colors = [
        FurColor(
            row["park_id"],
            row["squirrel_id"],
            row["primary_fur_color"],
            row["highlights_in_fur_color"],
            row["color_notes"],
        ).to_dict()
        for row in read_csv_file(
            squirrel_fur_color_filepath, schema=SQUIRREL_FUR_COLOR_SCHEMA
        ).collect()
    ]

    postgres_db.execute_batch(
        """
        INSERT INTO color (name)
        VALUES (%(name)s)
        ON CONFLICT (name)
        DO NOTHING
        """,
        colors,
        page_size=50,
    )

    postgres_db.execute_batch(
        """
        INSERT INTO squirrel
        (
            park_id, squirrel_id, location, above_ground, specific_location,
            interactions_with_humans, observations, latitude, longitude
        )
        VALUES
        (
            %(park_id)s, %(squirrel_id)s, %(location)s, %(above_ground)s, %(specific_location)s,
            %(interactions_with_humans)s, %(observations)s, %(latitude)s, %(longitude)s
        )
        ON CONFLICT (squirrel_id)
        DO UPDATE SET
            park_id = %(park_id)s,
            location = %(location)s,
            above_ground = %(above_ground)s,
            specific_location = %(specific_location)s,
            interactions_with_humans = %(interactions_with_humans)s,
            observations = %(observations)s,
            latitude = %(latitude)s,
            longitude = %(longitude)s
        """,
        squirrels,
        page_size=1000,
    )

    postgres_db.execute_batch(
        """
        INSERT INTO squirrel_fur_color
        (
            park_id, squirrel_id, primary_color, highlight_color, notes
        )
        VALUES
        (
            %(park_id)s, %(squirrel_id)s, %(primary_fur_color)s, %(highlights_in_fur_color)s, %(color_notes)s
        )
        ON CONFLICT (park_id, squirrel_id, primary_color, highlight_color)
        DO UPDATE SET
            primary_color = %(primary_fur_color)s,
            highlight_color = %(highlights_in_fur_color)s,
            notes = %(color_notes)s
        """,
        fur_colors,
        page_size=1000,
    )

    postgres_db.execute_batch(
        """
        INSERT INTO squirrel_activity (squirrel_id, activity_name)
        VALUES (%(squirrel_id)s, %(activity_name)s)
        ON CONFLICT (squirrel_id, activity_name)
        DO NOTHING
        """,
        squirrel_activities,
        page_size=1000,
    )
