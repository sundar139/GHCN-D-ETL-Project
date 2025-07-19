import tarfile
import os
import logging
from typing import List, Set
from io import StringIO

logger = logging.getLogger(__name__)

class FileExtractor:
    """Extract and filter files from GHCN-D tar archive"""
    
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.raw_path = os.path.join(base_path, "raw")
    
    def parse_stations_file(self, stations_file: str, target_state: str = "GA") -> Set[str]:
        """Parse stations file and return station IDs for target state"""
        station_ids = set()
        
        try:
            with open(stations_file, 'r') as f:
                for line in f:
                    if len(line.strip()) > 0:
                        # Parse fixed-width format
                        station_id = line[0:11].strip()
                        state = line[38:40].strip()
                        
                        if state == target_state:
                            station_ids.add(station_id)
            
            logger.info(f"Found {len(station_ids)} stations in {target_state}")
            return station_ids
            
        except Exception as e:
            logger.error(f"Error parsing stations file: {e}")
            raise
    
    def extract_station_files(self, tar_file: str, station_ids: Set[str], 
                             start_year: int = 2015, end_year: int = 2025) -> List[str]:
        """Extract .dly files for specified stations and date range"""
        extracted_files = []
        
        try:
            with tarfile.open(tar_file, 'r:gz') as tar:
                # Get all .dly files
                dly_files = [member for member in tar.getmembers() 
                           if member.name.endswith('.dly')]
                
                logger.info(f"Found {len(dly_files)} .dly files in archive")
                
                for member in dly_files:
                    # Extract station ID from filename
                    station_id = os.path.basename(member.name).replace('.dly', '')
                    
                    # Check if this station is in our target list
                    if station_id in station_ids:
                        # Extract file
                        tar.extract(member, self.raw_path)
                        
                        # Filter by date range
                        filtered_file = self._filter_by_date_range(
                            os.path.join(self.raw_path, member.name),
                            start_year, end_year
                        )
                        
                        if filtered_file:
                            extracted_files.append(filtered_file)
                
                logger.info(f"Extracted {len(extracted_files)} station files")
                return extracted_files
                
        except Exception as e:
            logger.error(f"Error extracting station files: {e}")
            raise
    
    def _filter_by_date_range(self, file_path: str, start_year: int, end_year: int) -> str:
        """Filter .dly file by date range"""
        try:
            filtered_lines = []
            
            with open(file_path, 'r') as f:
                for line in f:
                    if len(line.strip()) > 0:
                        # Extract year from position 11-15
                        year = int(line[11:15])
                        
                        # Keep lines within date range
                        if start_year <= year <= end_year:
                            filtered_lines.append(line)
            
            # Write filtered content back to file
            if filtered_lines:
                with open(file_path, 'w') as f:
                    f.writelines(filtered_lines)
                
                logger.info(f"Filtered {file_path} to {len(filtered_lines)} records")
                return file_path
            else:
                # Remove empty file
                os.remove(file_path)
                logger.info(f"Removed empty file {file_path}")
                return None
                
        except Exception as e:
            logger.error(f"Error filtering file {file_path}: {e}")
            return None
    
    def get_file_statistics(self, file_path: str) -> dict:
        """Get statistics about a .dly file"""
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
            
            if not lines:
                return {"record_count": 0, "date_range": None}
            
            # Parse first and last lines to get date range
            first_line = lines[0]
            last_line = lines[-1]
            
            start_year = int(first_line[11:15])
            start_month = int(first_line[15:17])
            end_year = int(last_line[11:15])
            end_month = int(last_line[15:17])
            
            return {
                "record_count": len(lines),
                "date_range": {
                    "start": f"{start_year}-{start_month:02d}",
                    "end": f"{end_year}-{end_month:02d}"
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting file statistics: {e}")
            return {"record_count": 0, "date_range": None}
