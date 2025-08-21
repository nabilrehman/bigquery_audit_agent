"""
Configuration file for Google Cloud Platform settings
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Google Cloud Project Configuration
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT', 'gcp-project-1755750305')
REGION = os.getenv('GOOGLE_CLOUD_REGION', 'us-central1')
ZONE = os.getenv('GOOGLE_CLOUD_ZONE', 'us-central1-a')

# Storage Configuration
DEFAULT_BUCKET = os.getenv('DEFAULT_BUCKET', 'my-storage-bucket')

# Compute Configuration
MACHINE_TYPE = os.getenv('MACHINE_TYPE', 'e2-micro')
IMAGE_FAMILY = os.getenv('IMAGE_FAMILY', 'debian-11')

# Functions Configuration
FUNCTION_REGION = os.getenv('FUNCTION_REGION', 'us-central1')

# Print configuration for debugging
def print_config():
    """Print current configuration"""
    print("=== GCP Configuration ===")
    print(f"Project ID: {PROJECT_ID}")
    print(f"Region: {REGION}")
    print(f"Zone: {ZONE}")
    print(f"Default Bucket: {DEFAULT_BUCKET}")
    print(f"Machine Type: {MACHINE_TYPE}")
    print(f"Image Family: {IMAGE_FAMILY}")
    print(f"Function Region: {FUNCTION_REGION}")
    print("========================")

if __name__ == "__main__":
    print_config()
