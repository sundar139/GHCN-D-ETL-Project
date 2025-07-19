import sys
sys.path.append('/Workspace/Users/rohithsundar.1392001@gmail.com/GHCN-D ETL Project/src')

from utils.config_loader import ConfigLoader
from transform.gold_processor import GoldProcessor
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
config_loader = ConfigLoader('/Workspace/Users/rohithsundar.1392001@gmail.com/GHCN-D ETL Project/config/pipeline_config.yaml')
config = config_loader.load_config()
print("Pipeline configuration loaded successfully")

# Initialize gold processor
gold_processor = GoldProcessor(spark, config)

print("Gold processor initialized")

# Load silver data
silver_path = config['storage']['silver_path']
silver_df = spark.read.format("delta").load(silver_path)

print("Silver data loaded:")
print(f"Total records: {silver_df.count()}")
print(f"Date range: {silver_df.agg(min('DATE'), max('DATE')).collect()[0]}")

# Process all gold layer tables
gold_processor.process_silver_to_gold()

print("Gold layer processing complete!")

# Load and examine monthly aggregates
monthly_path = f"{config['storage']['gold_path']}/monthly_climate"
monthly_df = spark.read.format("delta").load(monthly_path)

print("Monthly climate aggregates:")
print(f"Total records: {monthly_df.count()}")
print(f"Unique stations: {monthly_df.select('ID').distinct().count()}")

# Show sample data
print("\nSample monthly data:")
monthly_df.show(10)

# Monthly temperature patterns
print("Average monthly temperatures across all stations:")
monthly_df.groupBy("month").agg(
    avg("avg_temp").alias("avg_temp"),
    avg("total_precip").alias("avg_precip")
).orderBy("month").show()

# Load and examine yearly aggregates
yearly_path = f"{config['storage']['gold_path']}/yearly_climate"
yearly_df = spark.read.format("delta").load(yearly_path)

print("Yearly climate aggregates:")
print(f"Total records: {yearly_df.count()}")

# Show sample data
print("\nSample yearly data:")
yearly_df.show(10)

# Yearly trends
print("Yearly temperature trends:")
yearly_df.groupBy("year").agg(
    avg("avg_temp").alias("avg_temp"),
    avg("annual_precip").alias("avg_precip"),
    avg("hot_days").alias("avg_hot_days")
).orderBy("year").show()

# Load and examine climate summaries
climate_path = f"{config['storage']['gold_path']}/climate_summaries"
climate_df = spark.read.format("delta").load(climate_path)

print("Climate summaries:")
print(f"Total records: {climate_df.count()}")

# Show sample data
print("\nSample climate summaries:")
climate_df.show(10)

# Climate zones distribution
print("Climate zone distribution:")
climate_df.groupBy("climate_zone").count().show()

print("Precipitation regime distribution:")
climate_df.groupBy("precipitation_regime").count().show()

# Load and examine ML features
ml_path = f"{config['storage']['gold_path']}/ml_features"
ml_df = spark.read.format("delta").load(ml_path)

print("ML features dataset:")
print(f"Total records: {ml_df.count()}")
print(f"Number of features: {len(ml_df.columns)}")

# Show sample data
ml_df.show(10)

# Feature statistics
print("Feature statistics:")
numeric_cols = ["TMAX", "TMIN", "PRCP", "temp_range", "tmax_7day_avg", "tmin_7day_avg"]
ml_df.select(*numeric_cols).describe().show()

# Check data completeness across gold tables
gold_stats = gold_processor.get_gold_statistics()

print("Gold layer statistics:")
for table_name, stats in gold_stats.items():
    print(f"\n{table_name}:")
    for key, value in stats.items():
        if key != "column_names":
            print(f"  {key}: {value}")

# Analyze temperature trends over time
temp_trends = yearly_df.groupBy("year").agg(
    avg("avg_temp").alias("state_avg_temp"),
    avg("min_temp").alias("state_min_temp"),
    avg("max_temp").alias("state_max_temp")
).orderBy("year")

print("Georgia temperature trends:")
temp_trends.show()

# Analyze extreme weather events
extreme_weather = yearly_df.select(
    "year", "ID", "NAME", "hot_days", "freezing_days", "heavy_precip_days"
).filter(
    (col("hot_days") > 30) | (col("heavy_precip_days") > 10)
)

print("Stations with extreme weather events:")
extreme_weather.show()

# Analyze climate patterns by elevation
elevation_climate = climate_df.select(
    "ID", "NAME", "ELEVATION", "normal_temp", "normal_precip", "climate_zone"
).filter(col("ELEVATION").isNotNull())

print("Climate by elevation:")
elevation_climate.orderBy("ELEVATION").show()

# Export sample datasets for external analysis
export_path = f"{config['storage']['gold_path']}/exports"

# Export monthly aggregates as Parquet
monthly_df.coalesce(1).write.mode("overwrite").parquet(f"{export_path}/monthly_climate.parquet")

# Export ML features as Parquet
ml_df.coalesce(10).write.mode("overwrite").parquet(f"{export_path}/ml_features.parquet")

print("Sample datasets exported for external use")