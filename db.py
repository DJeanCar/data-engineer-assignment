import os
import psycopg2
import logging
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from logging_config import setup_logging

load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)


class PostgresDB:
    def __init__(self, dbname, user, password, host, port="5432") -> None:
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cur = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            self.cur = self.conn.cursor()
        except Exception as e:
            logger.error(f"Error: {e}")

    def create_initial_tables(self):
        try:
            if self.cur:
                self.cur.execute("SET search_path TO public;")
                self.cur.execute(
                    """
                CREATE TABLE IF NOT EXISTS area (
                    area_id VARCHAR(20) PRIMARY KEY,
                    area_name VARCHAR(150)
                );
                CREATE TABLE IF NOT EXISTS park (
                    area_id VARCHAR(20),
                    park_id DECIMAL PRIMARY KEY,
                    park_name VARCHAR(150),
                    date DATE,
                    start_time TIME,
                    end_time TIME,
                    total_time INTEGER,
                    park_conditions VARCHAR(150),
                    other_animal_sighlings VARCHAR(250),
                    litter VARCHAR(150),
                    temperature_weather VARCHAR(150),
                    
                    CONSTRAINT fk_area FOREIGN KEY(area_id) REFERENCES area(area_id)
                );

                CREATE TABLE IF NOT EXISTS color (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50) UNIQUE
                );

                CREATE TABLE IF NOT EXISTS squirrel (
                    park_id DECIMAL,
                    squirrel_id VARCHAR(50) PRIMARY KEY,
                    location VARCHAR(150),
                    above_ground VARCHAR(20),
                    specific_location VARCHAR(50),
                    interactions_with_humans VARCHAR(50),
                    observations VARCHAR(200),
                    latitude DECIMAL,
                    longitude DECIMAL,
                    
                    CONSTRAINT fk_park FOREIGN KEY(park_id) REFERENCES park(park_id)
                );
                CREATE TABLE IF NOT EXISTS squirrel_fur_color (
                    park_id DECIMAL,
                    squirrel_id VARCHAR(50),
                    primary_color INTEGER,
                    highlight_color INTEGER,
                    notes VARCHAR(150),
                    
                    UNIQUE(park_id, squirrel_id, primary_color, highlight_color),
                    CONSTRAINT fk_park FOREIGN KEY(park_id) REFERENCES park(park_id),
                    CONSTRAINT fk_squirrel FOREIGN KEY(squirrel_id) REFERENCES squirrel(squirrel_id),
                    CONSTRAINT fk_primary_color FOREIGN KEY(primary_color) REFERENCES color(id),
                    CONSTRAINT fk_highlight_color FOREIGN KEY(highlight_color) REFERENCES color(id)
                );
                CREATE TABLE IF NOT EXISTS animal_sighling (
                    park_id DECIMAL,
                    animal_name VARCHAR(50),
                    
                    UNIQUE(park_id, animal_name)
                );
                CREATE TABLE IF NOT EXISTS squirrel_activity (
                    squirrel_id VARCHAR(50),
                    activity_name VARCHAR(50),
                    
                    UNIQUE(squirrel_id, activity_name)
                );
                """
                )
                self.conn.commit()
                logger.debug("Tables created successfully (if it did not already exist)")
            else:
                logger.debug("Cursor is not initialized.")
        except Exception as e:
            logger.error(f"Error: {e}")

    def select_query(self, query):
        try:
            if self.cur:
                self.cur.execute(query)
                return self.cur.fetchall()
            else:
                logger.debug("Cursor is not initialized.")
        except Exception as e:
            logger.error(f"Error: {e}")
    
    def execute_batch(self, query, data, page_size = 1000):
        """
        Execute a batch of SQL queries.

        :param query: SQL query with placeholders.
        :param data: List of tuples with data to insert or update.
        """
        try:
            if self.cur:
                execute_batch(self.cur, query, data, page_size=page_size)
                self.conn.commit()
                logger.debug(f"{len(data)} records inserted or updated successfully")
            else:
                logger.debug("Cursor is not initialized.")
        except Exception as e:
            logger.error(f"Error: {e}")

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        logger.info("Connection closed")


postgres_db = PostgresDB(
    os.getenv("DB_NAME"),
    os.getenv("DB_USER"),
    os.getenv("DB_PASSWORD"),
    os.getenv("DB_HOST"),
    os.getenv("DB_PORT"),
)
