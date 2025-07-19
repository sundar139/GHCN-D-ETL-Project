from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class SparkUtils:
    """Spark utilities for GHCN-D ETL pipeline"""
    
    @staticmethod
    def configure_spark_session(spark_config: Dict[str, str]) -> SparkSession:
        """Configure Spark session with optimizations"""
        spark = SparkSession.builder.appName("GHCN-D ETL Pipeline")
        
        # Apply configuration settings
        for key, value in spark_config.items():
            spark = spark.config(key, value)
        
        return spark.getOrCreate()
    
    @staticmethod
    def optimize_dataframe(df: DataFrame, partition_columns: List[str] = None) -> DataFrame:
        """Optimize DataFrame for better performance"""
        # Cache if beneficial
        if df.count() > 10000:  # Cache larger datasets
            df = df.cache()
        
        # Repartition if partition columns specified
        if partition_columns:
            df = df.repartition(*[col(c) for c in partition_columns])
        
        return df
    
    @staticmethod
    def write_with_optimization(df: DataFrame, path: str, format: str = "delta", 
                               mode: str = "overwrite", partition_by: List[str] = None) -> None:
        """Write DataFrame with optimizations"""
        writer = df.write.format(format).mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        # Apply optimizations
        writer = writer.option("optimizeWrite", "true")
        writer = writer.option("autoCompact", "true")
        
        writer.save(path)
        logger.info(f"Data written to {path} with format {format}")
    
    @staticmethod
    def create_checkpoint(df: DataFrame, checkpoint_path: str) -> DataFrame:
        """Create checkpoint for fault tolerance"""
        df.write.mode("overwrite").option("checkpointLocation", checkpoint_path).save()
        return df
    
    @staticmethod
    def batch_process(df: DataFrame, batch_size: int, process_func) -> DataFrame:
        """Process DataFrame in batches"""
        total_rows = df.count()
        num_batches = (total_rows + batch_size - 1) // batch_size
        
        processed_dfs = []
        
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_rows)
            
            # Create batch
            batch_df = df.limit(end_idx).offset(start_idx)
            
            # Process batch
            processed_batch = process_func(batch_df)
            processed_dfs.append(processed_batch)
            
            logger.info(f"Processed batch {i+1}/{num_batches}")
        
        # Union all processed batches
        result_df = processed_dfs[0]
        for df_batch in processed_dfs[1:]:
            result_df = result_df.union(df_batch)
        
        return result_df
