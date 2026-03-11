#!/bin/bash

# Simple script to deploy the metadata_evolution view using environment variables
# Requires: GOOGLE_CLOUD_PROJECT and GOOGLE_CLOUD_PROJECT_NUMBER

if [ -z "$GOOGLE_CLOUD_PROJECT" ] || [ -z "$GOOGLE_CLOUD_PROJECT_NUMBER" ]; then
  echo "Error: GOOGLE_CLOUD_PROJECT and GOOGLE_CLOUD_PROJECT_NUMBER must be set."
  echo "Usage: export GOOGLE_CLOUD_PROJECT=my-project"
  echo "       export GOOGLE_CLOUD_PROJECT_NUMBER=1234567890"
  echo "       ./deploy_evolution_view.sh"
  exit 1
fi

echo "Deploying Metadata Evolution View to project: $GOOGLE_CLOUD_PROJECT"
echo "Using Project Number: $GOOGLE_CLOUD_PROJECT_NUMBER"

# Create a temporary file with resolved values
sed "s/\${PROJECT_ID}/$GOOGLE_CLOUD_PROJECT/g; s/\${PROJECT_NUMBER}/$GOOGLE_CLOUD_PROJECT_NUMBER/g" dataplex_integration/metadata_evolution_view.sql > /tmp/resolved_evolution_view.sql

# Execute the query
bq query --use_legacy_sql=false < /tmp/resolved_evolution_view.sql

if [ $? -eq 0 ]; then
  echo "View successfully deployed!"
else
  echo "Error deploying view."
fi

rm /tmp/resolved_evolution_view.sql
