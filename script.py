import logging
from schema import (
    PARK_SCHEMA,
    SQUIRREL_SCHEMA,
    PARK_COLUMNS_RENAME_MAP,
    SQUIRREL_COLUMNS_RENAME_MAP,
)
from utils.csv import read_csv_file, rename_csv_columns
from process_data import (
    process_park_data,
    process_squirrel_data,
    proccess_park_dataframes,
    proccess_squirrel_dataframes,
)
from db import postgres_db
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

postgres_db.connect()
postgres_db.create_initial_tables()
postgres_db.close()

PARK_FILEPATH = "./data/raw/park-data.csv"
SQUIRREL_FILEPATH = "./data/raw/squirrel-data.csv"

PROCESSED_FOLDERPATH = "./data/processed"

park_data = rename_csv_columns(
    read_csv_file(PARK_FILEPATH, PARK_SCHEMA), PARK_COLUMNS_RENAME_MAP
)
squirrel_data = rename_csv_columns(
    read_csv_file(SQUIRREL_FILEPATH, SQUIRREL_SCHEMA), SQUIRREL_COLUMNS_RENAME_MAP
)

postgres_db.connect()

try:
    proccess_park_dataframes(park_data, PROCESSED_FOLDERPATH)
    proccess_squirrel_dataframes(squirrel_data, PROCESSED_FOLDERPATH)
except Exception as e:
    logger.debug(f"Error generator processed csv files: {e}")


process_park_data(PROCESSED_FOLDERPATH)
process_squirrel_data(PROCESSED_FOLDERPATH)


# questions to answer
"""
1. How many squirrels are three in each park?
"""
squirrels_by_park = postgres_db.select_query(
    """
select p.park_id, p.park_name, count(*) from squirrel as s
inner join park as p on p.park_id = s.park_id
group by p.park_id
order by p.park_id;                         
"""
)

logger.info("1. How many squirrels are three in each park?------\n")
logger.info(squirrels_by_park)

"""
2. How many squirrels are there in each Borough?
"""
squirrels_by_area = postgres_db.select_query(
    """
select a.area_id, a.area_name, count(*) from squirrel s
inner join park as p on p.park_id = s.park_id
inner join area as a on a.area_id = p.area_id
group by a.area_id                       
"""
)

logger.info("\n\n2. How many squirrels are there in each Borough?---------\n")
logger.info(squirrels_by_area)


"""
3. A count of "Other Animal Sightings" by Park.
"""
animal_sighlings = postgres_db.select_query(
    """
select p.park_id, p.park_name , count(*) from animal_sighling as as2
inner join park as p on p.park_id = as2.park_id
where as2.animal_name <> 'humans'
group by p.park_id;                     
"""
)

logger.info("\n\n3. A count of 'Other Animal Sightings' by Park.---------\n")
logger.info(animal_sighlings)


"""
4. What is the most common activity for Squirrels? (e.g. eating, running, etc..)
"""
most_common_activity = postgres_db.select_query(
    """
select activity_name, SUM(1) as ctn from squirrel_activity
group by activity_name
order by ctn desc
limit 1;                   
"""
)

logger.info(
    "\n\n4. What is the most common activity for Squirrels? (e.g. eating, running, etc..)---------\n"
)
logger.info(most_common_activity)


"""
5. A count of all Primary Fur Colors by Park.
"""
count_for_primary_fur_color = postgres_db.select_query(
    """
with color_without_duplicates as
(
  select park_id, primary_color from (
    select park_id, squirrel_id, primary_color from squirrel_fur_color
    group by park_id, squirrel_id, primary_color
  )
  group by park_id, primary_color
)
select p.park_id, p.park_name, count(*) from color_without_duplicates as cwd
inner join park as p on p.park_id = cwd.park_id
group by p.park_id;           
"""
)

logger.info("\n\n5. A count of all Primary Fur Colors by Park.---------\n")
logger.info(count_for_primary_fur_color)

postgres_db.close()
