"""
Example Cloud Function for Google Cloud Functions
This function can be deployed to GCP and triggered via HTTP requests
"""

import functions_framework
from google.cloud import storage
import json

@functions_framework.http
def hello_gcp(request):
    """HTTP Cloud Function that lists storage buckets"""
    
    # Set CORS headers for web requests
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)
    
    headers = {'Access-Control-Allow-Origin': '*'}
    
    try:
        # Initialize storage client
        storage_client = storage.Client()
        
        # List all buckets
        buckets = list(storage_client.list_buckets())
        bucket_names = [bucket.name for bucket in buckets]
        
        # Create response
        response_data = {
            'message': 'Hello from Google Cloud Functions!',
            'buckets': bucket_names,
            'bucket_count': len(bucket_names),
            'timestamp': functions_framework.datetime.datetime.now().isoformat()
        }
        
        return (json.dumps(response_data), 200, headers)
        
    except Exception as e:
        error_response = {
            'error': str(e),
            'message': 'An error occurred while processing the request'
        }
        return (json.dumps(error_response), 500, headers)

# For local testing
if __name__ == "__main__":
    # This allows you to test the function locally
    print("Cloud Function Example")
    print("This function can be deployed to Google Cloud Functions")
    print("Use 'gcloud functions deploy' to deploy it")
