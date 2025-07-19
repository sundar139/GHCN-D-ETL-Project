import sys
import os  # Add this import statement
sys.path.append('/Workspace/Users/rohithsundar.1392001@gmail.com/GHCN-D ETL Project/src')

from utils.config_loader import ConfigLoader
from utils.data_validator import DataValidator
from transform.silver_processor import SilverProcessor
from pyspark.sql.functions import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
config_loader = ConfigLoader('/Workspace/Users/rohithsundar.1392001@gmail.com/GHCN-D ETL Project/config/pipeline_config.yaml')
config = config_loader.load_config()
print("Pipeline configuration loaded successfully")

# Initialize processors
silver_processor = SilverProcessor(spark, config)
data_validator = DataValidator(config)

print("Silver processor initialized")

# Load bronze data
bronze_path = config['storage']['bronze_path']
bronze_df = spark.read.format("delta").load(bronze_path)

print("Bronze data loaded:")
print(f"Total records: {bronze_df.count()}")
print(f"Unique stations: {bronze_df.select('ID').distinct().count()}")

# Show element distribution
print("\nElement distribution:")
bronze_df.groupBy("ELEMENT").count().orderBy("count", ascending=False).show()

# Process bronze data to silver
silver_df = silver_processor.process_bronze_to_silver()

print(f"Silver processing complete: {silver_df.count()} records")

# Show sample of silver data
print("Sample of silver data:")
silver_df.show(10)

# Show schema
print("Silver layer schema:")
silver_df.printSchema()

# Validate temperature ranges
validated_df = data_validator.validate_temperature_range(silver_df)
validated_df = data_validator.validate_precipitation(validated_df)
validated_df = data_validator.calculate_quality_score(validated_df)

# Check completeness
completeness = data_validator.check_data_completeness(validated_df)
print("Data completeness by element:")
for element, percentage in completeness.items():
    print(f"{element}: {percentage:.1f}%")

# Check data quality distribution
print("Data quality score distribution:")
validated_df.select("data_quality_score").describe().show()

# Show records with low quality scores
low_quality = validated_df.filter(col("data_quality_score") < 0.5)
print(f"Records with quality score < 0.5: {low_quality.count()}")

# Analyze station distribution
print("Station distribution by state:")
validated_df.groupBy("STATE").count().show()

# Elevation distribution
print("Station elevation statistics:")
validated_df.select("ELEVATION").describe().show()

# Geographic distribution
print("Sample of station locations:")
validated_df.select("ID", "NAME", "LATITUDE", "LONGITUDE", "ELEVATION").distinct().show(10)

# Temperature analysis
print("Temperature statistics:")
validated_df.select("TMAX", "TMIN").describe().show()

# Precipitation analysis
print("Precipitation statistics:")
validated_df.select("PRCP").describe().show()

# Monthly temperature patterns
print("Average temperature by month:")
validated_df.groupBy("month").agg(
    avg("TMAX").alias("avg_tmax"),
    avg("TMIN").alias("avg_tmin")
).orderBy("month").show()

# Save to Silver layer
silver_processor.save_silver_data(validated_df)

# Get statistics
silver_stats = silver_processor.get_silver_statistics()
print("Silver layer statistics:")
for key, value in silver_stats.items():
    print(f"{key}: {value}")

# Verify saved data
saved_silver_df = spark.read.format("delta").load(config['storage']['silver_path'])

print("Verification of saved silver data:")
print(f"Record count: {saved_silver_df.count()}")

# Show sample data
print("\nSample of saved data:")
saved_silver_df.show(10)