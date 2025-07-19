import sys
sys.path.append('/Workspace/Users/rohithsundar.1392001@gmail.com/GHCN-D ETL Project/src')

from utils.config_loader import ConfigLoader
from utils.spark_utils import SparkUtils
from transform.bronze_processor import BronzeProcessor
import logging
import os
import glob
from pyspark.sql.functions import col  # Add this import

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
config_loader = ConfigLoader('/Workspace/Users/rohithsundar.1392001@gmail.com/GHCN-D ETL Project/config/pipeline_config.yaml')
config = config_loader.load_config()

print("Pipeline configuration loaded successfully")

# Initialize bronze processor
bronze_processor = BronzeProcessor(spark, config)

# Get list of .dly files to process
raw_path = config['storage']['raw_path']
dly_files = glob.glob(os.path.join(raw_path, "*.dly"))

print(f"Found {len(dly_files)} .dly files to process")
print(f"Sample files: {dly_files[:3]}")

# Process files in batches to manage memory
batch_size = config['processing']['batch_size']
total_files = len(dly_files)
batch_count = (total_files + batch_size - 1) // batch_size

print(f"Processing {total_files} files in {batch_count} batches")

# Process first batch to test
test_batch = dly_files[:min(5, len(dly_files))]
bronze_df = bronze_processor.process_dly_files(test_batch)

# Show sample of processed data
print("Sample of processed bronze data:")
display(bronze_df)


# Show schema
print("Bronze layer schema:")
bronze_df.printSchema()

# Process all files
all_bronze_df = bronze_processor.process_dly_files(dly_files)

print(f"Total bronze records: {all_bronze_df.count()}")

# Basic data quality checks
print("Bronze layer statistics:")
print(f"Total records: {all_bronze_df.count()}")
print(f"Unique stations: {all_bronze_df.select('ID').distinct().count()}")
print(f"Date range: {all_bronze_df.agg({'year': 'min'}).collect()[0][0]} - {all_bronze_df.agg({'year': 'max'}).collect()[0][0]}")

# Check elements distribution
print("\nElement distribution:")
all_bronze_df.groupBy("ELEMENT").count().orderBy("count", ascending=False).show()

# Check for data quality issues
print("Data quality checks:")

# Check for missing values
missing_values = all_bronze_df.filter(col("VALUE").isNull()).count()
print(f"Missing values: {missing_values}")

# Check for extreme values
extreme_tps = all_bronze_df.filter(
    (col("ELEMENT").isin(["TMAX", "TMIN"])) & 
    ((col("VALUE") < -500) | (col("VALUE") > 500))
).count()
print(f"Extreme temperature values: {extreme_tps}")

# Save to Bronze layer
bronze_processor.save_bronze_data(all_bronze_df)

# Get statistics
bronze_stats = bronze_processor.get_bronze_statistics()
print("Bronze layer statistics:")
for key, value in bronze_stats.items():
    print(f"{key}: {value}")

# Verify saved data
saved_bronze_df = spark.read.format("delta").load(config['storage']['bronze_path'])

print("Verification of saved bronze data:")
print(f"Record count: {saved_bronze_df.count()}")

# Show sample data
print("\nSample of saved data:")
saved_bronze_df.show(10)