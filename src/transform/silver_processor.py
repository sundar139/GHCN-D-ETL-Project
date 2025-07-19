from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import os

logger = logging.getLogger(__name__)

class SilverProcessor:
    """Process Bronze layer data into Silver layer with cleaning and enrichment"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.stations_path = config.get('storage', {}).get('stations_path')
        self.bronze_path = config.get('storage', {}).get('bronze_path')
        self.silver_path = config.get('storage', {}).get('silver_path')
        self.raw_path = config.get('storage', {}).get('raw_path')
        self.required_elements = config.get('processing', {}).get('required_elements', [])
    
    def process_bronze_to_silver(self) -> DataFrame:
        """Process bronze data into silver layer"""
        try:
            # Load bronze data
            bronze_df = self.spark.read.format("delta").load(self.bronze_path)
            
            # Filter required elements
            filtered_df = bronze_df.filter(col("ELEMENT").isin(self.required_elements))
            
            # Convert units and clean data
            cleaned_df = self._clean_and_convert_units(filtered_df)
            
            # Pivot elements to columns
            pivoted_df = self._pivot_elements(cleaned_df)
            
            # Load and join station metadata
            stations_df = self._load_station_metadata()
            enriched_df = self._join_station_metadata(pivoted_df, stations_df)
            
            # Add data quality metrics
            quality_df = self._add_quality_metrics(enriched_df)
            
            logger.info(f"Silver processing complete: {quality_df.count()} records")
            return quality_df
            
        except Exception as e:
            logger.error(f"Error in silver processing: {e}")
            raise
    
    def _clean_and_convert_units(self, df: DataFrame) -> DataFrame:
        """Clean data and convert units"""
        df = df.withColumn(
            "VALUE",
            when(col("ELEMENT").isin(["TMAX", "TMIN"]), col("VALUE") / 10.0)
            .when(col("ELEMENT").isin(["PRCP", "SNOW", "SNWD"]), col("VALUE") / 10.0)
            .otherwise(col("VALUE"))
        )

        df = df.withColumn(
            "VALUE",
            when(
                (col("ELEMENT").isin(["TMAX", "TMIN"])) &
                ((col("VALUE") < -50.0) | (col("VALUE") > 50.0)),
                None
            ).when(
                (col("ELEMENT") == "PRCP") &
                ((col("VALUE") < 0.0) | (col("VALUE") > 200.0)),
                None
            ).otherwise(col("VALUE"))
        )

        df = df.withColumn(
            "DATE",
            to_date(concat(col("year"), lpad(col("month"), 2, "0"), lpad(col("day"), 2, "0")), "yyyyMMdd")
        )

        return df

    def _pivot_elements(self, df: DataFrame) -> DataFrame:
        """Pivot weather elements from rows to columns"""
        pivoted_df = df.groupBy("ID", "DATE", "year", "month", "day") \
                      .pivot("ELEMENT", self.required_elements) \
                      .agg(first("VALUE"))
        return pivoted_df

    def _load_station_metadata(self) -> DataFrame:
        """Load station metadata from stations file"""
        try:
            # Use the CORRECTED stations file path (no subdirectory)
            stations_file = os.path.join(self.stations_path, "ghcnd-stations.txt")
            
            # Optional: file existence check
            if not os.path.exists(stations_file):
                raise FileNotFoundError(f"Stations file not found at {stations_file}")

            # Read fixed-width text file as lines
            lines_df = self.spark.read.text(stations_file)

            # Parse fixed-width fields using substring expressions
            stations_df = lines_df.select(
                trim(substring("value", 1, 11)).alias("ID"),
                substring("value", 13, 8).cast("double").alias("LATITUDE"),
                substring("value", 22, 9).cast("double").alias("LONGITUDE"),
                substring("value", 32, 6).cast("double").alias("ELEVATION"),
                trim(substring("value", 39, 2)).alias("STATE"),
                trim(substring("value", 42, 30)).alias("NAME"),
                trim(substring("value", 82, 2)).alias("COUNTRY")
            )

            return stations_df

        except Exception as e:
            logger.error(f"Error loading station metadata: {e}")
            raise

    def _join_station_metadata(self, df: DataFrame, stations_df: DataFrame) -> DataFrame:
        """Join weather data with station metadata"""
        joined_df = df.join(stations_df, "ID", "left")
        return joined_df

    def _add_quality_metrics(self, df: DataFrame) -> DataFrame:
        """Add data quality metrics"""
        df = df.withColumn(
            "data_quality_score",
            (col("TMAX").isNotNull().cast("int") +
             col("TMIN").isNotNull().cast("int") +
             col("PRCP").isNotNull().cast("int") +
             col("SNOW").isNotNull().cast("int") +
             col("SNWD").isNotNull().cast("int")) / 5.0
        )
        df = df.withColumn(
            "temp_consistent",
            when(col("TMAX").isNull() | col("TMIN").isNull(), True)
            .when(col("TMAX") >= col("TMIN"), True)
            .otherwise(False)
        )
        df = df.withColumn(
            "data_quality_score",
            when(col("temp_consistent") == False, col("data_quality_score") * 0.8)
            .otherwise(col("data_quality_score"))
        )
        return df.drop("temp_consistent")

    def save_silver_data(self, df: DataFrame) -> None:
        """Save processed data to Silver layer"""
        try:
            df.write \
              .format("delta") \
              .mode("overwrite") \
              .partitionBy("year", "month") \
              .option("optimizeWrite", "true") \
              .option("autoCompact", "true") \
              .save(self.silver_path)
            logger.info(f"Silver data saved to {self.silver_path}")
        except Exception as e:
            logger.error(f"Error saving silver data: {e}")
            raise

    def get_silver_statistics(self) -> dict:
        """Get statistics about silver layer data"""
        try:
            df = self.spark.read.format("delta").load(self.silver_path)
            stats = {
                "total_records": df.count(),
                "unique_stations": df.select("ID").distinct().count(),
                "date_range": df.agg(
                    min("DATE").alias("min_date"),
                    max("DATE").alias("max_date")
                ).collect()[0].asDict(),
                "avg_quality_score": df.agg(avg("data_quality_score")).collect()[0][0],
                "completeness": {
                    "TMAX": df.filter(col("TMAX").isNotNull()).count() / df.count(),
                    "TMIN": df.filter(col("TMIN").isNotNull()).count() / df.count(),
                    "PRCP": df.filter(col("PRCP").isNotNull()).count() / df.count(),
                    "SNOW": df.filter(col("SNOW").isNotNull()).count() / df.count(),
                    "SNWD": df.filter(col("SNWD").isNotNull()).count() / df.count()
                }
            }
            return stats
        except Exception as e:
            logger.error(f"Error getting silver statistics: {e}")
            return {}
