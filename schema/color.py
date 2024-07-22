from pyspark.sql.types import StructType, StructField, StringType, IntegerType

COLOR_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])
