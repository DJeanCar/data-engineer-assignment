from utils.date import parse_str_to_date, parse_str_to_time
from entity.area import AreaBuilder
from entity.park import ParkBuilder
from entity.animal_sighling import AnimalSighling
from dataframes import get_area_dt, get_park_dt, get_animal_sighling_df
from pyspark.sql import DataFrame
from db import postgres_db
from pathlib import Path
from utils.csv import save_csv_file, read_csv_file
from schema.processed_files import PARK_SCHEMA, AREA_SCHEMA, ANIMAL_SIGHLING_SCHEMA

TEMP_DIRECTORY = "temp"
AREA_PROCESSED_FILENAME = "area.csv"
PARK_PROCESSED_FILENAME = "park.csv"
ANIMAL_SIGHLING_PROCESSED_FILENAME = "animal_sighling.csv"


def proccess_park_dataframes(park_data: DataFrame, processed_folderpath: str):
    folder_path = Path(processed_folderpath)
    temp_directory = folder_path / TEMP_DIRECTORY

    area_df = get_area_dt(park_data)
    park_df = get_park_dt(park_data)
    animal_sighling_df = get_animal_sighling_df(park_data)

    save_csv_file(area_df, temp_directory, folder_path / AREA_PROCESSED_FILENAME)
    save_csv_file(park_df, temp_directory, folder_path / PARK_PROCESSED_FILENAME)
    save_csv_file(
        animal_sighling_df,
        temp_directory,
        folder_path / ANIMAL_SIGHLING_PROCESSED_FILENAME,
    )


def process_park_data(processed_folderpath: str):
    folder_path = Path(processed_folderpath)
    
    areas = [
        AreaBuilder()
        .set_id(row["area_id"])
        .set_name(row["area_name"])
        .build()
        .to_dict()
        for row in read_csv_file(str(folder_path / "area.csv"), schema=AREA_SCHEMA).collect()
    ]

    parks = [
        ParkBuilder()
        .set_park_details(row["area_id"], row["park_id"], row["park_name"])
        .set_date_times(
            parse_str_to_date(row["date"]),
            parse_str_to_time(row["start_time"]),
            parse_str_to_time(row["end_time"]),
            row["total_time"],
        )
        .set_aditional_info(
            row["park_conditions"],
            row["other_animal_sightings"],
            row["litter"],
            row["temperature_weather"],
        )
        .build()
        .to_dict()
        for row in read_csv_file(str(folder_path / "park.csv"), schema=PARK_SCHEMA).collect()
    ]

    animal_sighlings = [
        AnimalSighling(row["park_id"], row["other_animal_sightings"]).to_dict()
        for row in read_csv_file(str(folder_path / "animal_sighling.csv"), schema=ANIMAL_SIGHLING_SCHEMA).collect()
    ]

    postgres_db.execute_batch(
        """
        INSERT INTO area (area_id, area_name)
        VALUES (%(area_id)s, %(area_name)s)
        ON CONFLICT (area_id)
        DO UPDATE SET area_name = %(area_name)s
        """,
        areas,
        page_size=1000,
    )

    postgres_db.execute_batch(
        """
        INSERT INTO park 
        (
            area_id, park_id, park_name, date, start_time, end_time, total_time,
            park_conditions, other_animal_sighlings, litter, temperature_weather
        )
        VALUES
        (
            %(area_id)s, %(park_id)s, %(park_name)s, %(date)s, %(start_time)s, %(end_time)s, %(total_time)s,
            %(park_conditions)s, %(other_animal_sighlings)s, %(litter)s, %(temperature_weather)s
        )
        ON CONFLICT (park_id)
        DO UPDATE SET
            park_name = %(park_name)s,
            date = %(date)s,
            start_time = %(start_time)s,
            end_time = %(end_time)s,
            total_time = %(total_time)s,
            park_conditions = %(park_conditions)s,
            other_animal_sighlings = %(other_animal_sighlings)s,
            litter = %(litter)s,
            temperature_weather = %(temperature_weather)s
        """,
        parks,
        page_size=1000,
    )

    postgres_db.execute_batch(
        """
        INSERT INTO animal_sighling (park_id, animal_name)
        VALUES (%(park_id)s, %(animal_name)s)
        ON CONFLICT (park_id, animal_name)
        DO NOTHING
        """,
        animal_sighlings,
        page_size=1000,
    )
