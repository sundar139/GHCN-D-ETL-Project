from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    
    def __init__(self, config: Dict):
        self.config = config
        self.quality_checks = config.get('quality_checks', {})
    
    def validate_temperature_range(self, df: DataFrame) -> DataFrame:
        temp_range = self.quality_checks.get('temperature_range', {})
        min_temp = temp_range.get('min', -500)
        max_temp = temp_range.get('max', 500)

        df = df.withColumn(
            "valid_tmax",
            when(col("TMAX").isNull(), True)
            .when((col("TMAX") >= min_temp) & (col("TMAX") <= max_temp), True)
            .otherwise(False)
        )
        
        df = df.withColumn(
            "valid_tmin",
            when(col("TMIN").isNull(), True)
            .when((col("TMIN") >= min_temp) & (col("TMIN") <= max_temp), True)
            .otherwise(False)
        )
        
        return df
    
    def validate_precipitation(self, df: DataFrame) -> DataFrame:
        max_precip = self.quality_checks.get('precipitation_max', 2000)
        
        df = df.withColumn(
            "valid_prcp",
            when(col("PRCP").isNull(), True)
            .when((col("PRCP") >= 0) & (col("PRCP") <= max_precip), True)
            .otherwise(False)
        )
        
        return df
    
    def calculate_quality_score(self, df: DataFrame) -> DataFrame:
        quality_columns = ["valid_tmax", "valid_tmin", "valid_prcp"]

        df = df.withColumn(
            "data_quality_score",
            (col("valid_tmax").cast("int") + 
             col("valid_tmin").cast("int") + 
             col("valid_prcp").cast("int")) / 3.0
        )

        df = df.drop(*quality_columns)
        
        return df
    
    def validate_schema(self, df: DataFrame, expected_schema) -> bool:
        try:
            df_fields = {field.name: field.dataType for field in df.schema.fields}
            expected_fields = {field.name: field.dataType for field in expected_schema.fields}
            
            missing_fields = set(expected_fields.keys()) - set(df_fields.keys())
            if missing_fields:
                logger.error(f"Missing fields: {missing_fields}")
                return False
            
            type_mismatches = []
            for field_name, expected_type in expected_fields.items():
                if field_name in df_fields and df_fields[field_name] != expected_type:
                    type_mismatches.append(f"{field_name}: expected {expected_type}, got {df_fields[field_name]}")
            
            if type_mismatches:
                logger.error(f"Type mismatches: {type_mismatches}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Schema validation error: {e}")
            return False
    
    def check_data_completeness(self, df: DataFrame) -> Dict[str, float]:
        total_rows = df.count()
        
        completeness = {}
        key_columns = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD"]
        
        for col_name in key_columns:
            if col_name in df.columns:
                non_null_count = df.filter(col(col_name).isNotNull()).count()
                completeness[col_name] = (non_null_count / total_rows) * 100 if total_rows > 0 else 0
        
        return completeness
    
    def detect_outliers(self, df: DataFrame, column: str) -> DataFrame:
        quantiles = df.select(column).filter(col(column).isNotNull()).approxQuantile(column, [0.25, 0.75], 0.05)
        
        if len(quantiles) == 2:
            Q1, Q3 = quantiles
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            df = df.withColumn(
                f"{column}_outlier",
                when(col(column).isNull(), False)
                .when((col(column) < lower_bound) | (col(column) > upper_bound), True)
                .otherwise(False)
            )
        
        return df
