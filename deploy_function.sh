#!/bin/bash

# Deploy Cloud Function to Google Cloud Functions
# Make sure you're authenticated and have the correct project set

echo "Deploying Cloud Function to Google Cloud Functions..."

# Set variables
FUNCTION_NAME="hello-gcp"
REGION="us-central1"
RUNTIME="python39"
TRIGGER="http"

# Deploy the function
gcloud functions deploy $FUNCTION_NAME \
    --runtime=$RUNTIME \
    --region=$REGION \
    --source=. \
    --entry-point=hello_gcp \
    --trigger=$TRIGGER \
    --allow-unauthenticated

echo "Function deployment completed!"
echo "You can test it at the URL provided above"
echo ""
echo "To test locally, run:"
echo "functions-framework --target=hello_gcp --debug"
