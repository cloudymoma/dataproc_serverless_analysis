#!/bin/bash

# Get a list of all available Dataproc regions
REGIONS=$(gcloud compute regions list --format="value(name)")

PROJECT_ID=du-hast-mich

# Loop through each region to fetch and process batch jobs
#for region in $REGIONS; do
  gcloud dataproc batches list \
    --project=${PROJECT_ID} \
    --region="us-central1" \
    --filter='create_time >= "2025-07-01T00:00:00Z" AND create_time <= "2025-07-14T23:59:59Z"' \
    --format=json
#done
