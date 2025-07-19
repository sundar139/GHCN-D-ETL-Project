import sys
sys.path.append('/Workspace/Users/rohithsundar.1392001@gmail.com/GHCN-D ETL Project/src')

from utils.config_loader import ConfigLoader
from utils.data_validator import DataValidator
from utils.schema_definitions import GHCNSchemas
from pyspark.sql.functions import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
config_loader = ConfigLoader('/Workspace/Users/rohithsundar.1392001@gmail.com/GHCN-D ETL Project/config/pipeline_config.yaml')
config = config_loader.load_config()
print("Pipeline configuration loaded successfully")

# Instantiate DataValidator
data_validator = DataValidator(config)

# Load data from each layer
bronze_df = spark.read.format("delta").load(config['storage']['bronze_path'])
silver_df = spark.read.format("delta").load(config['storage']['silver_path'])

# Validate schemas
bronze_schema_valid = data_validator.validate_schema(bronze_df, GHCNSchemas.BRONZE_SCHEMA)
silver_schema_valid = data_validator.validate_schema(silver_df, GHCNSchemas.SILVER_SCHEMA)

print(f"Bronze schema validation: {'✓ PASS' if bronze_schema_valid else '✗ FAIL'}")
print(f"Silver schema validation: {'✓ PASS' if silver_schema_valid else '✗ FAIL'}")

print("Bronze Layer Data Quality Assessment")
print("=" * 40)

# Basic statistics
print(f"Total records: {bronze_df.count():,}")
print(f"Unique stations: {bronze_df.select('ID').distinct().count()}")
print(f"Date range: {bronze_df.agg(min('year'), max('year')).collect()[0]}")

# Element distribution
print("\nElement distribution:")
bronze_df.groupBy("ELEMENT").count().orderBy("count", ascending=False).show()

# Check for missing values
missing_values = bronze_df.filter(col("VALUE").isNull()).count()
print(f"Missing values: {missing_values:,}")

print("Silver Layer Data Quality Assessment")
print("=" * 40)

# Basic statistics
print(f"Total records: {silver_df.count():,}")
print(f"Unique stations: {silver_df.select('ID').distinct().count()}")

# Data completeness
completeness = data_validator.check_data_completeness(silver_df)
print("\nData completeness by element:")
for element, percentage in completeness.items():
    print(f"  {element}: {percentage:.1f}%")

# Quality score analysis
print("\nData quality score distribution:")
silver_df.select("data_quality_score").describe().show()

# Identify low quality records
low_quality_threshold = 0.3
low_quality_count = silver_df.filter(col("data_quality_score") < low_quality_threshold).count()
print(f"Records with quality score < {low_quality_threshold}: {low_quality_count:,}")

print("Temperature Validation")
print("=" * 20)

# Temperature range validation
temp_validated = data_validator.validate_temperature_range(silver_df)
temp_validated = data_validator.validate_precipitation(temp_validated)
temp_validated = data_validator.calculate_quality_score(temp_validated)

# Check for temperature anomalies
temp_anomalies = silver_df.filter(
    (col("TMAX") < -30) | (col("TMAX") > 45) | 
    (col("TMIN") < -35) | (col("TMIN") > 30)
).count()

print(f"Temperature anomalies: {temp_anomalies:,}")

# Temperature consistency (TMAX >= TMIN)
temp_inconsistent = silver_df.filter(
    col("TMAX").isNotNull() & col("TMIN").isNotNull() & (col("TMAX") < col("TMIN"))
).count()

print(f"Temperature inconsistencies (TMAX < TMIN): {temp_inconsistent:,}")

print("Precipitation Validation")
print("=" * 23)

# Precipitation statistics
prcp_stats = silver_df.select("PRCP").filter(col("PRCP").isNotNull()).describe()
prcp_stats.show()

# Extreme precipitation events
extreme_prcp = silver_df.filter(col("PRCP") > 100).count()  # > 100mm
print(f"Extreme precipitation events (>100mm): {extreme_prcp:,}")

# Negative precipitation values
negative_prcp = silver_df.filter(col("PRCP") < 0).count()
print(f"Negative precipitation values: {negative_prcp:,}")

# Load monthly aggregates
monthly_path = f"{config['storage']['gold_path']}/monthly_climate"
monthly_df = spark.read.format("delta").load(monthly_path)

print("Monthly Aggregates Validation")
print("=" * 29)

print(f"Total records: {monthly_df.count():,}")
print(f"Unique stations: {monthly_df.select('ID').distinct().count()}")

# Check for missing months
expected_months = 12
actual_months = monthly_df.select("month").distinct().count()
print(f"Months covered: {actual_months}/12")

# Temperature aggregates validation
temp_agg_issues = monthly_df.filter(
    col("min_temp") > col("max_temp")
).count()
print(f"Temperature aggregation issues: {temp_agg_issues:,}")

# Load yearly aggregates
yearly_path = f"{config['storage']['gold_path']}/yearly_climate"
yearly_df = spark.read.format("delta").load(yearly_path)

print("Yearly Aggregates Validation")
print("=" * 27)

print(f"Total records: {yearly_df.count():,}")

# Check year coverage
expected_years = list(range(config['processing']['start_year'], config['processing']['end_year'] + 1))
actual_years = [row.year for row in yearly_df.select("year").distinct().collect()]
missing_years = set(expected_years) - set(actual_years)

print(f"Years covered: {len(actual_years)}/{len(expected_years)}")
if missing_years:
    print(f"Missing years: {missing_years}")

# Load ML features
ml_path = f"{config['storage']['gold_path']}/ml_features"
ml_df = spark.read.format("delta").load(ml_path)

print("ML Features Validation")
print("=" * 21)

print(f"Total records: {ml_df.count():,}")
print(f"Number of features: {len(ml_df.columns)}")

# Check for missing values in key features
key_features = ["TMAX", "TMIN", "PRCP", "tmax_7day_avg", "tmin_7day_avg"]
for feature in key_features:
    if feature in ml_df.columns:
        missing_count = ml_df.filter(col(feature).isNull()).count()
        missing_pct = (missing_count / ml_df.count()) * 100
        print(f"  {feature}: {missing_pct:.1f}% missing")

print("Data Lineage Validation")
print("=" * 23)

# Record count validation across layers
bronze_count = bronze_df.count()
silver_count = silver_df.count()
monthly_count = monthly_df.count()

print(f"Bronze records: {bronze_count:,}")
print(f"Silver records: {silver_count:,}")
print(f"Monthly records: {monthly_count:,}")

# Expected transformations
expected_silver_count = bronze_df.groupBy("ID", "DATE").count().count()
print(f"Expected silver records: {expected_silver_count:,}")

# Station consistency
bronze_stations = set([row.ID for row in bronze_df.select("ID").distinct().collect()])
silver_stations = set([row.ID for row in silver_df.select("ID").distinct().collect()])
monthly_stations = set([row.ID for row in monthly_df.select("ID").distinct().collect()])

print(f"Stations - Bronze: {len(bronze_stations)}, Silver: {len(silver_stations)}, Monthly: {len(monthly_stations)}")

print("Performance Validation")
print("=" * 21)

# Check partition distribution
bronze_partitions = len(bronze_df.inputFiles())
silver_partitions = len(silver_df.inputFiles())
monthly_partitions = len(monthly_df.inputFiles())

print(f"Partitions - Bronze: {bronze_partitions}, Silver: {silver_partitions}, Monthly: {monthly_partitions}")

# Check file sizes (approximate)
def get_data_file_info(df, layer_name):
    files = df.inputFiles()
    print(f"{layer_name} input files: {len(files)}")
    if len(files):
        print(f"{layer_name} example files:")
        for f in files[:3]:
            print(f"  {f}")

get_data_file_info(bronze_df, "Bronze")
get_data_file_info(silver_df, "Silver")

print("GHCN-D ETL Pipeline Validation Summary")
print("=" * 40)

# Create validation report
validation_report = {
    "schema_validation": {
        "bronze_schema": "✓ PASS" if bronze_schema_valid else "✗ FAIL",
        "silver_schema": "✓ PASS" if silver_schema_valid else "✗ FAIL"
    },
    "data_quality": {
        "bronze_records": bronze_count,
        "silver_records": silver_count,
        "monthly_records": monthly_count,
        "temperature_anomalies": temp_anomalies,
        "temperature_inconsistent": temp_inconsistent,
        "extreme_precipitation": extreme_prcp,
        "negative_precipitation": negative_prcp
    },
    "completeness": completeness,
    "performance": {
        "bronze_partitions": bronze_partitions,
        "silver_partitions": silver_partitions,
        "monthly_partitions": monthly_partitions
    }
}

# Display summary
for category, metrics in validation_report.items():
    print(f"\n{category.upper()}:")
    for metric, value in metrics.items():
        print(f"  {metric}: {value}")

print("Data Quality Recommendations")
print("=" * 31)

recommendations = []

# Data quality recommendations
if temp_anomalies > 100:
    recommendations.append("⚠️  High number of temperature anomalies detected - review data sources")

if temp_inconsistent > 0:
    recommendations.append("⚠️  Temperature inconsistencies found - implement stricter validation")

if any(completeness[elem] < 80 for elem in completeness):
    recommendations.append("⚠️  Low data completeness for some elements - consider data imputation")

if bronze_partitions > 200:
    recommendations.append("⚠️  Too many partitions may cause small file problem - consider repartitioning")

if not recommendations:
    recommendations.append("✅ All validation checks passed - data quality is good")

for rec in recommendations:
    print(rec)