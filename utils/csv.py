import glob
import os
import shutil
from pyspark.sql import DataFrame
from typing import Dict
from spark_init import spark
from pyspark.sql.types import StructType


def read_csv_file(file_path: str, schema: StructType) -> DataFrame:
    return (
        spark.read.format("csv").option("header", True).schema(schema).load(file_path)
    )


def rename_csv_columns(df: DataFrame, columns_rename_map: Dict[str, str]) -> DataFrame:
    for original_col, new_col in columns_rename_map.items():
        df = df.withColumnRenamed(original_col, new_col)
    return df


def save_csv_file(df: DataFrame, temp_directory: str, final_output_path: str):
    df.coalesce(1).write.csv(str(temp_directory), mode='overwrite', header=True)

    part_files = glob.glob(str(temp_directory / "part-*.csv"))
    if part_files:
        os.rename(part_files[0], final_output_path)
        shutil.rmtree(temp_directory)
