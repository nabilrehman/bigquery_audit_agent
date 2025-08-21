#!/usr/bin/env python3
"""
Google Cloud Platform Examples
This file demonstrates various GCP services usage
"""

import os
from google.cloud import storage
from google.cloud import compute_v1
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError

def list_buckets():
    """List all storage buckets in the current project"""
    try:
        client = storage.Client()
        buckets = list(client.list_buckets())
        print(f"Found {len(buckets)} buckets:")
        for bucket in buckets:
            print(f"  - {bucket.name}")
        return buckets
    except DefaultCredentialsError:
        print("Error: No default credentials found. Run 'gcloud auth application-default login'")
        return []

def list_instances():
    """List all compute instances in the current project"""
    try:
        client = compute_v1.InstancesClient()
        project = os.getenv('GOOGLE_CLOUD_PROJECT', 'gcp-project-1755750305')
        zone = 'us-central1-a'  # Default zone
        
        request = compute_v1.ListInstancesRequest(
            project=project,
            zone=zone
        )
        
        instances = client.list(request=request)
        print(f"Instances in zone {zone}:")
        for instance in instances:
            print(f"  - {instance.name} ({instance.status})")
        return instances
    except Exception as e:
        print(f"Error listing instances: {e}")
        return []

def create_bucket(bucket_name):
    """Create a new storage bucket"""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        bucket.create()
        print(f"Bucket {bucket_name} created successfully!")
        return bucket
    except Exception as e:
        print(f"Error creating bucket: {e}")
        return None

def get_project_info():
    """Get current project information"""
    try:
        credentials, project = default()
        print(f"Current project: {project}")
        print(f"Credentials type: {type(credentials).__name__}")
        return project
    except DefaultCredentialsError:
        print("Error: No default credentials found. Run 'gcloud auth application-default login'")
        return None

def main():
    """Main function to demonstrate GCP services"""
    print("=== Google Cloud Platform Examples ===\n")
    
    # Get project info
    print("1. Project Information:")
    project = get_project_info()
    print()
    
    # List buckets
    print("2. Storage Buckets:")
    list_buckets()
    print()
    
    # List compute instances
    print("3. Compute Instances:")
    list_instances()
    print()
    
    # Example: Create a bucket (uncomment to use)
    # print("4. Creating a test bucket:")
    # create_bucket("test-bucket-example")
    # print()

if __name__ == "__main__":
    main()
