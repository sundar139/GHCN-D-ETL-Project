# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from typing import List
import logging
import os

logger = logging.getLogger(__name__)

class GoldProcessor:
    """Process Silver layer data into Gold layer with aggregations and ML features"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.silver_path = config.get('storage', {}).get('silver_path')
        self.gold_path = config.get('storage', {}).get('gold_path')
    
    def process_silver_to_gold(self) -> None:
        """Process silver data into gold layer with multiple views"""
        try:
            # Load silver data
            silver_df = self.spark.read.format("delta").load(self.silver_path)
            
            # Generate monthly aggregates
            monthly_df = self._create_monthly_aggregates(silver_df)
            self._save_gold_table(monthly_df, "monthly_climate")
            
            # Generate yearly aggregates
            yearly_df = self._create_yearly_aggregates(silver_df)
            self._save_gold_table(yearly_df, "yearly_climate")
            
            # Generate climate summaries
            climate_df = self._create_climate_summaries(silver_df)
            self._save_gold_table(climate_df, "climate_summaries")
            
            # Generate ML features
            ml_df = self._create_ml_features(silver_df)
            self._save_gold_table(ml_df, "ml_features")
            
            logger.info("Gold layer processing complete")
            
        except Exception as e:
            logger.error(f"Error in gold processing: {e}")
            raise
    
    def _create_monthly_aggregates(self, df: DataFrame) -> DataFrame:
        """Create monthly climate aggregates"""
        monthly_df = df.groupBy("ID", "year", "month", "LATITUDE", "LONGITUDE", 
                               "ELEVATION", "STATE", "NAME") \
                      .agg(
                          # Temperature aggregates
                          avg("TMAX").alias("avg_tmax"),
                          avg("TMIN").alias("avg_tmin"),
                          avg((col("TMAX") + col("TMIN")) / 2).alias("avg_temp"),
                          min("TMIN").alias("min_temp"),
                          max("TMAX").alias("max_temp"),
                          
                          # Precipitation aggregates
                          sum("PRCP").alias("total_precip"),
                          avg("PRCP").alias("avg_precip"),
                          max("PRCP").alias("max_precip"),
                          
                          # Snow aggregates
                          sum("SNOW").alias("total_snow"),
                          avg("SNOW").alias("avg_snow"),
                          max("SNOW").alias("max_snow"),
                          max("SNWD").alias("max_snow_depth"),
                          
                          # Count metrics
                          count("*").alias("record_count"),
                          sum(when(col("PRCP") > 0, 1).otherwise(0)).alias("days_with_precip"),
                          sum(when(col("SNOW") > 0, 1).otherwise(0)).alias("days_with_snow"),
                          sum(when(col("SNWD") > 0, 1).otherwise(0)).alias("days_with_snow_cover"),
                          
                          # Quality metrics
                          avg("data_quality_score").alias("avg_quality_score")
                      )
        
        # Add derived metrics
        monthly_df = monthly_df.withColumn(
            "temperature_range", col("max_temp") - col("min_temp")
        ).withColumn(
            "precip_days_pct", col("days_with_precip") / col("record_count") * 100
        ).withColumn(
            "snow_days_pct", col("days_with_snow") / col("record_count") * 100
        )
        
        return monthly_df
    
    def _create_yearly_aggregates(self, df: DataFrame) -> DataFrame:
        """Create yearly climate aggregates"""
        yearly_df = df.groupBy("ID", "year", "LATITUDE", "LONGITUDE", 
                              "ELEVATION", "STATE", "NAME") \
                     .agg(
                         # Temperature statistics
                         avg("TMAX").alias("avg_tmax"),
                         avg("TMIN").alias("avg_tmin"),
                         avg((col("TMAX") + col("TMIN")) / 2).alias("avg_temp"),
                         min("TMIN").alias("min_temp"),
                         max("TMAX").alias("max_temp"),
                         
                         # Precipitation statistics
                         sum("PRCP").alias("annual_precip"),
                         avg("PRCP").alias("avg_daily_precip"),
                         max("PRCP").alias("max_daily_precip"),
                         
                         # Snow statistics
                         sum("SNOW").alias("annual_snow"),
                         max("SNOW").alias("max_daily_snow"),
                         max("SNWD").alias("max_snow_depth"),
                         
                         # Extreme weather days
                         sum(when(col("TMAX") > 32, 1).otherwise(0)).alias("hot_days"),
                         sum(when(col("TMIN") < 0, 1).otherwise(0)).alias("freezing_days"),
                         sum(when(col("PRCP") > 25, 1).otherwise(0)).alias("heavy_precip_days"),
                         
                         # Quality and completeness
                         count("*").alias("record_count"),
                         avg("data_quality_score").alias("avg_quality_score")
                     )
        
        # Add growing season metrics
        yearly_df = yearly_df.withColumn(
            "growing_season_length", 
            365 - col("freezing_days")  # Simplified growing season
        ).withColumn(
            "heat_stress_days", col("hot_days")
        ).withColumn(
            "moisture_index", col("annual_precip") / 1000  # Simplified moisture index
        )
        
        return yearly_df
    
    def _create_climate_summaries(self, df: DataFrame) -> DataFrame:
        """Create climate summary statistics across all years"""
        # Calculate climate normals (averages across years)
        climate_df = df.groupBy("ID", "month", "LATITUDE", "LONGITUDE", 
                               "ELEVATION", "STATE", "NAME") \
                      .agg(
                          # Temperature normals
                          avg("TMAX").alias("normal_tmax"),
                          avg("TMIN").alias("normal_tmin"),
                          avg((col("TMAX") + col("TMIN")) / 2).alias("normal_temp"),
                          
                          # Precipitation normals
                          avg("PRCP").alias("normal_precip"),
                          
                          # Variability measures
                          stddev("TMAX").alias("tmax_stddev"),
                          stddev("TMIN").alias("tmin_stddev"),
                          stddev("PRCP").alias("precip_stddev"),
                          
                          # Extreme statistics
                          min("TMIN").alias("record_low"),
                          max("TMAX").alias("record_high"),
                          max("PRCP").alias("record_precip"),
                          
                          # Data availability
                          count("*").alias("total_observations"),
                          countDistinct("year").alias("years_of_data")
                      )
        
        # Add climate classification indicators
        climate_df = climate_df.withColumn(
            "climate_zone",
            when(col("normal_temp") > 20, "Hot")
            .when(col("normal_temp") > 10, "Temperate")
            .when(col("normal_temp") > 0, "Cool")
            .otherwise("Cold")
        ).withColumn(
            "precipitation_regime",
            when(col("normal_precip") > 5, "Wet")
            .when(col("normal_precip") > 2, "Moderate")
            .otherwise("Dry")
        )
        
        return climate_df
    
    def _create_ml_features(self, df: DataFrame) -> DataFrame:
        """Create ML-ready features dataset"""
        # Window specification for time-based features
        window_spec = Window.partitionBy("ID").orderBy("DATE")
        
        # Create features with lags and rolling averages
        ml_df = df.withColumn(
            "tmax_lag1", lag("TMAX", 1).over(window_spec)
        ).withColumn(
            "tmin_lag1", lag("TMIN", 1).over(window_spec)
        ).withColumn(
            "prcp_lag1", lag("PRCP", 1).over(window_spec)
        ).withColumn(
            "tmax_7day_avg", avg("TMAX").over(window_spec.rowsBetween(-6, 0))
        ).withColumn(
            "tmin_7day_avg", avg("TMIN").over(window_spec.rowsBetween(-6, 0))
        ).withColumn(
            "prcp_7day_sum", sum("PRCP").over(window_spec.rowsBetween(-6, 0))
        ).withColumn(
            "temp_range", col("TMAX") - col("TMIN")
        ).withColumn(
            "day_of_year", dayofyear("DATE")
        ).withColumn(
            "month_sin", sin(col("month") * 2 * 3.14159 / 12)
        ).withColumn(
            "month_cos", cos(col("month") * 2 * 3.14159 / 12)
        )
        
        # Add anomaly features (deviation from monthly normal)
        monthly_normals = df.groupBy("ID", "month").agg(
            avg("TMAX").alias("monthly_normal_tmax"),
            avg("TMIN").alias("monthly_normal_tmin"),
            avg("PRCP").alias("monthly_normal_prcp")
        )
        
        ml_df = ml_df.join(monthly_normals, ["ID", "month"], "left")
        
        ml_df = ml_df.withColumn(
            "tmax_anomaly", col("TMAX") - col("monthly_normal_tmax")
        ).withColumn(
            "tmin_anomaly", col("TMIN") - col("monthly_normal_tmin")
        ).withColumn(
            "prcp_anomaly", col("PRCP") - col("monthly_normal_prcp")
        )
        
        # Select final feature columns
        feature_columns = [
            "ID", "DATE", "year", "month", "day", "day_of_year",
            "LATITUDE", "LONGITUDE", "ELEVATION", "STATE",
            "TMAX", "TMIN", "PRCP", "SNOW", "SNWD",
            "tmax_lag1", "tmin_lag1", "prcp_lag1",
            "tmax_7day_avg", "tmin_7day_avg", "prcp_7day_sum",
            "temp_range", "tmax_anomaly", "tmin_anomaly", "prcp_anomaly",
            "month_sin", "month_cos", "data_quality_score"
        ]
        
        return ml_df.select(*feature_columns)
    
    def _save_gold_table(self, df: DataFrame, table_name: str) -> None:
        """Save gold layer table"""
        try:
            table_path = f"{self.gold_path}/{table_name}"
            
            # Determine partitioning strategy
            if "year" in df.columns and "month" in df.columns:
                partition_cols = ["year", "month"]
            elif "year" in df.columns:
                partition_cols = ["year"]
            else:
                partition_cols = None
            
            writer = df.write.format("delta").mode("overwrite")
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.option("optimizeWrite", "true") \
                  .option("autoCompact", "true") \
                  .save(table_path)
            
            logger.info(f"Gold table {table_name} saved to {table_path}")
            
        except Exception as e:
            logger.error(f"Error saving gold table {table_name}: {e}")
            raise
    
    def get_gold_statistics(self) -> dict:
        """Get statistics about gold layer data"""
        try:
            stats = {}
            
            # Get statistics for each gold table
            gold_tables = ["monthly_climate", "yearly_climate", "climate_summaries", "ml_features"]
            
            for table_name in gold_tables:
                try:
                    table_path = f"{self.gold_path}/{table_name}"
                    df = self.spark.read.format("delta").load(table_path)
                    
                    stats[table_name] = {
                        "record_count": df.count(),
                        "unique_stations": df.select("ID").distinct().count() if "ID" in df.columns else 0,
                        "columns": len(df.columns),
                        "column_names": df.columns
                    }
                    
                except Exception as e:
                    logger.warning(f"Could not get statistics for {table_name}: {e}")
                    stats[table_name] = {"error": str(e)}
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting gold statistics: {e}")
            return {}
