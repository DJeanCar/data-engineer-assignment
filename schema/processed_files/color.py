from pyspark.sql.types import StructType, StructField, StringType

COLOR_SCHEMA = StructType([
    StructField("color", StringType(), True)
])
