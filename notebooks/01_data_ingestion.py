import sys
sys.path.append('/Workspace/Users/rohithsundar.1392001@gmail.com/GHCN-D ETL Project/src')

from utils.config_loader import ConfigLoader
from ingest.data_downloader import DataDownloader
from ingest.file_extractor import FileExtractor
import logging
import os

# CONFIGURATION
config_loader = ConfigLoader('/Workspace/Users/rohithsundar.1392001@gmail.com/GHCN-D ETL Project/config/pipeline_config.yaml')
config = config_loader.load_config()
print("Pipeline configuration loaded successfully")

base_url = config['data_sources']['base_url']  # e.g., 'https://www.ncei.noaa.gov/pub/data/ghcn/daily/'
stations_file = config['data_sources']['stations_file']  # 'ghcnd-stations.txt'
daily_data_file = config['data_sources']['daily_data_file']  # 'ghcnd_all.tar.gz'

stations_url = base_url.rstrip('/') + '/' + stations_file
daily_data_url = base_url.rstrip('/') + '/' + daily_data_file

print(f"Stations file URL: {stations_url}")
print(f"Daily data file URL: {daily_data_url}")

# DATA DOWNLOAD
downloader = DataDownloader(config['storage']['base_path'])

# Download stations file
stations_file_path = downloader.download_file(
    stations_url,
    stations_file
)
if downloader.verify_file_integrity(stations_file_path):
    print(f"✓ Stations file downloaded and verified: {stations_file_path}")
else:
    raise Exception("Stations file failed integrity check")

# Download daily data archive
daily_data_file_path = downloader.download_file(
    daily_data_url,
    daily_data_file
)
if downloader.verify_file_integrity(daily_data_file_path):
    print(f"✓ Daily data archive downloaded and verified: {daily_data_file_path}")
else:
    raise Exception("Daily data archive failed integrity check")

# Initialize extractor
extractor = FileExtractor(config['storage']['base_path'])

# Parse stations file to get Georgia station IDs
target_state = config['processing']['target_state']
georgia_stations = extractor.parse_stations_file(stations_file_path, target_state)

print(f"Found {len(georgia_stations)} stations in {target_state}")
print(f"Sample station IDs: {list(georgia_stations)[:5]}")

# Extract .dly files for Georgia stations
start_year = config['processing']['start_year']
end_year = config['processing']['end_year']

extracted_files = extractor.extract_station_files(
    daily_data_file_path, 
    georgia_stations, 
    start_year, 
    end_year
)

print(f"Extracted {len(extracted_files)} station files")

# Get statistics for extracted files
total_records = 0
date_ranges = []

for file_path in extracted_files[:5]:  # Sample first 5 files
    stats = extractor.get_file_statistics(file_path)
    total_records += stats['record_count']
    if stats['date_range']:
        date_ranges.append(stats['date_range'])
    print(f"File: {file_path}")
    print(f"  Records: {stats['record_count']}")
    print(f"  Date range: {stats['date_range']}")
    print()

print(f"Total records processed (sample): {total_records}")
print(f"Data ingestion completed successfully!")