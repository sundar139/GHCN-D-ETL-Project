from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
import logging
import os

logger = logging.getLogger(__name__)

class BronzeProcessor:
    """Process raw .dly files into Bronze layer structured format"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.raw_path = config.get('storage', {}).get('raw_path')
        self.bronze_path = config.get('storage', {}).get('bronze_path')
    
    def process_dly_files(self, dly_files: List[str]) -> DataFrame:
        """Process multiple .dly files into structured DataFrame"""
        all_dfs = []
        
        for file_path in dly_files:
            try:
                df = self._parse_single_dly_file(file_path)
                if df is not None:
                    all_dfs.append(df)
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                continue
        
        if not all_dfs:
            raise ValueError("No valid .dly files processed")
        
        # Union all DataFrames
        result_df = all_dfs[0]
        for df in all_dfs[1:]:
            result_df = result_df.union(df)
        
        logger.info(f"Processed {len(all_dfs)} .dly files into {result_df.count()} records")
        return result_df
    
    def _parse_single_dly_file(self, file_path: str) -> DataFrame:
        """Parse a single .dly file into structured format"""
        try:
            # Read file as text
            text_df = self.spark.read.text(file_path)
            
            # Parse fixed-width format
            parsed_df = text_df.select(
                # Station ID (positions 1-11)
                substring(col("value"), 1, 11).alias("ID"),
                # Year (positions 12-15)
                substring(col("value"), 12, 4).cast("int").alias("year"),
                # Month (positions 16-17)
                substring(col("value"), 16, 2).cast("int").alias("month"),
                # Element (positions 18-21)
                substring(col("value"), 18, 4).alias("ELEMENT"),
                # Full line for day parsing
                col("value").alias("raw_line")
            )
            
            # Explode days (each line contains 31 days of data)
            days_df = self._explode_daily_values(parsed_df)
            
            # Filter out invalid days and add date column
            final_df = days_df.filter(col("day").between(1, 31)) \
                             .withColumn("DATE", 
                                       to_date(concat(col("year"), 
                                                    lpad(col("month"), 2, "0"), 
                                                    lpad(col("day"), 2, "0")), 
                                             "yyyyMMdd"))
            
            # Filter out invalid dates
            final_df = final_df.filter(col("DATE").isNotNull())
            
            return final_df
            
        except Exception as e:
            logger.error(f"Error parsing {file_path}: {e}")
            return None
    
    def _explode_daily_values(self, df: DataFrame) -> DataFrame:
        """Explode daily values from wide format to long format"""
        # Create array of day numbers (1-31)
        days_array = array(*[lit(i) for i in range(1, 32)])
        
        # Explode to create one row per day
        exploded_df = df.select(
            col("ID"),
            col("year"),
            col("month"),
            col("ELEMENT"),
            col("raw_line"),
            explode(days_array).alias("day")
        )
        
        # Calculate positions for each day's data
        # Each day has 8 characters: VALUE(5) + MFLAG(1) + QFLAG(1) + SFLAG(1)
        exploded_df = exploded_df.withColumn(
            "start_pos", 21 + (col("day") - 1) * 8 + 1
        )
        
        # Extract daily values
        result_df = exploded_df.select(
            col("ID"),
            col("year"),
            col("month"),
            col("day"),
            col("ELEMENT"),
            # VALUE (5 characters)
            substring(col("raw_line"), col("start_pos"), 5).cast("int").alias("VALUE"),
            # MFLAG (1 character)
            substring(col("raw_line"), col("start_pos") + 5, 1).alias("MFLAG"),
            # QFLAG (1 character)
            substring(col("raw_line"), col("start_pos") + 6, 1).alias("QFLAG"),
            # SFLAG (1 character)
            substring(col("raw_line"), col("start_pos") + 7, 1).alias("SFLAG")
        )
        
        # Filter out missing values (-9999)
        result_df = result_df.filter(col("VALUE") != -9999)
        
        return result_df
    
    def save_bronze_data(self, df: DataFrame) -> None:
        """Save processed data to Bronze layer"""
        try:
            # Partition by year and month for efficient querying
            df.write \
              .format("delta") \
              .mode("overwrite") \
              .partitionBy("year", "month") \
              .option("optimizeWrite", "true") \
              .option("autoCompact", "true") \
              .save(self.bronze_path)
            
            logger.info(f"Bronze data saved to {self.bronze_path}")
            
        except Exception as e:
            logger.error(f"Error saving bronze data: {e}")
            raise
    
    def get_bronze_statistics(self) -> dict:
        """Get statistics about bronze layer data"""
        try:
            df = self.spark.read.format("delta").load(self.bronze_path)
            
            stats = {
                "total_records": df.count(),
                "unique_stations": df.select("ID").distinct().count(),
                "date_range": df.agg(
                    min("year").alias("min_year"),
                    max("year").alias("max_year")
                ).collect()[0].asDict(),
                "elements": df.select("ELEMENT").distinct().collect()
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting bronze statistics: {e}")
            return {}
