from pyspark.sql.types import *

class GHCNSchemas:

    STATION_SCHEMA = StructType([
        StructField("ID", StringType(), False),
        StructField("LATITUDE", DoubleType(), True),
        StructField("LONGITUDE", DoubleType(), True),
        StructField("ELEVATION", DoubleType(), True),
        StructField("STATE", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("COUNTRY", StringType(), True)
    ])

    BRONZE_SCHEMA = StructType([
        StructField("ID", StringType(), False),
        StructField("DATE", DateType(), False),
        StructField("ELEMENT", StringType(), False),
        StructField("VALUE", IntegerType(), True),
        StructField("MFLAG", StringType(), True),
        StructField("QFLAG", StringType(), True),
        StructField("SFLAG", StringType(), True),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False)
    ])

    SILVER_SCHEMA = StructType([
        StructField("ID", StringType(), False),
        StructField("DATE", DateType(), False),
        StructField("LATITUDE", DoubleType(), True),
        StructField("LONGITUDE", DoubleType(), True),
        StructField("ELEVATION", DoubleType(), True),
        StructField("STATE", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("TMAX", DoubleType(), True),
        StructField("TMIN", DoubleType(), True),
        StructField("PRCP", DoubleType(), True),
        StructField("SNOW", DoubleType(), True),
        StructField("SNWD", DoubleType(), True),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
        StructField("data_quality_score", DoubleType(), True)
    ])

    GOLD_MONTHLY_SCHEMA = StructType([
        StructField("ID", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("LATITUDE", DoubleType(), True),
        StructField("LONGITUDE", DoubleType(), True),
        StructField("ELEVATION", DoubleType(), True),
        StructField("STATE", StringType(), True),
        StructField("avg_temp", DoubleType(), True),
        StructField("min_temp", DoubleType(), True),
        StructField("max_temp", DoubleType(), True),
        StructField("total_precip", DoubleType(), True),
        StructField("avg_snow", DoubleType(), True),
        StructField("max_snow_depth", DoubleType(), True),
        StructField("days_with_precip", IntegerType(), True),
        StructField("days_with_snow", IntegerType(), True),
        StructField("record_count", IntegerType(), True)
    ])
