import requests
import os
import logging
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class DataDownloader:
    """Download GHCN-D data files via HTTPS"""
    
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.raw_path = os.path.join(base_path, "raw")
        os.makedirs(self.raw_path, exist_ok=True)
    
    def download_file(self, url: str, filename: Optional[str] = None, 
                     force_download: bool = False) -> str:
        """Download file from URL with intelligent caching"""
        if filename is None:
            filename = os.path.basename(urlparse(url).path)
        
        file_path = os.path.join(self.raw_path, filename)
        
        # Check if file already exists
        if os.path.exists(file_path) and not force_download:
            logger.info(f"File {filename} already exists, skipping download")
            return file_path
        
        logger.info(f"Downloading {url} to {file_path}")
        
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Get file size for progress tracking
            total_size = int(response.headers.get('content-length', 0))
            
            with open(file_path, 'wb') as f:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Log progress for large files
                        if total_size > 0 and downloaded % (1024 * 1024 * 10) == 0:  # Every 10MB
                            progress = (downloaded / total_size) * 100
                            logger.info(f"Download progress: {progress:.1f}%")
            
            logger.info(f"Successfully downloaded {filename}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            # Clean up partial file
            if os.path.exists(file_path):
                os.remove(file_path)
            raise
    
    def verify_file_integrity(self, file_path: str) -> bool:
        """Verify file integrity after download"""
        try:
            file_size = os.path.getsize(file_path)
            
            # Basic checks
            if file_size == 0:
                logger.error(f"File {file_path} is empty")
                return False
            
            # Check if it's a valid text file for stations
            if file_path.endswith('.txt'):
                with open(file_path, 'r') as f:
                    first_line = f.readline()
                    if len(first_line.strip()) == 0:
                        logger.error(f"File {file_path} appears to be empty or corrupted")
                        return False
            
            # Check if it's a valid tar.gz file
            elif file_path.endswith('.tar.gz'):
                import tarfile
                try:
                    with tarfile.open(file_path, 'r:gz') as tar:
                        # Try to read first file
                        tar.getnames()[:1]
                except:
                    logger.error(f"File {file_path} is not a valid tar.gz file")
                    return False
            
            logger.info(f"File {file_path} integrity verified")
            return True
            
        except Exception as e:
            logger.error(f"Error verifying file integrity: {e}")
            return False
