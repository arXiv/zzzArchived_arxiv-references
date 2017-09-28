#!/bin/bash

# Only attempt to get secrets if the env variable is set.
if [ "$SECRETS_BUCKET_NAME" != "" ]; then
    eval $(aws s3 cp s3://${SECRETS_BUCKET_NAME}/references-secrets - | sed 's/^/export /')
fi

uwsgi --http 0.0.0.0:8000 -w wsgi --processes 1 --threads 8
