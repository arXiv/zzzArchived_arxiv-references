Deployment
**********

This will get the whole system up and running locally, for dev/testing
purposes. Before you start, be sure to up your memory allocation for the
Docker VM. Grobid is the greediest of all, and will exit on OOM. I use 8GB
on my machine, but you could probably get away with less.

.. code-block:: bash

   # First, build everything!
   git clone git@github.com:cul-it/arxiv-reflink.git
   cd arxiv-reflink

   docker build ./ -t arxiv/references-api:latest -f ./Dockerfile-api
   docker build ./ -t arxiv/references-worker:latest -f ./Dockerfile-worker
   docker build ./ -t arxiv/references-api:agent -f ./Dockerfile-agent

   cd refextract
   docker build ./ -t arxiv/refextract:latest

   cd ../cermine
   docker build ./ -t arxiv/cermine:latest

   # We also need redis, localstack:
   docker pull redis
   docker pull localstack

   # Spin up a network for the system to run on.
   docker network create test

   # Start redis and localstack on the network.
   docker run --network=test \
        --name=the-redis \
        -d redis

   # Port 8080 is the localstack UI. You don't need to map these ports, per se.
   docker run -d -p 4567-4578:4567-4578 \
        -p 8080:8080 \
        --network=test \
        --name=localstack \
        -e "USE_SSL=true" \
        atlassianlabs/localstack

   # Start the extractors.
   docker run -d --network=test --name=refextract arxiv/refextract:latest
   docker run -d --network=test --name=cermine arxiv/cermine:latest

   # Grobid is available via the arXiv ECS Repository. You'll need to get
   # access and pull that image.
   docker run -d --network=test --name=grobid \
            [repository id].dkr.ecr.us-east-1.amazonaws.com/arxiv/grobid

   # Start the worker. Set the AWS credentials to something meaningless (but
   # set them, lest boto3 complain). Note that these endpoints are names
   # on the ``test`` network, set when we ran each respective image.
   docker run -d --network=test \
       -e "REDIS_ENDPOINT=the-redis:6379" \
       -e "AWS_ACCESS_KEY_ID=foo" \
       -e "AWS_SECRET_ACCESS_KEY=bar" \
       -e "CLOUDWATCH_ENDPOINT=https://localstack:4582" \
       -e "DYNAMODB_ENDPOINT=https://localstack:4569" \
       -e "CERMINE_ENDPOINT=http://cermine:8000" \
       -e "REFEXTRACT_ENDPOINT=http://refextract:8000" \
       -e "DYNAMODB_VERIFY=false" \
       -e "CLOUDWATCH_VERIFY=false" \
       -e "GROBID_PORT=8080" \
       -e "GROBID_HOSTNAME=grobid" \
       -e "LOGLEVEL=10" \
       arxiv/references-worker:latest

   # Start the API. We map port 8000 so that we can test it locally.
   docker run -d --network=test \
       -e "REDIS_ENDPOINT=the-redis:6379" \
       -e "AWS_ACCESS_KEY_ID=foo" \
       -e "AWS_SECRET_ACCESS_KEY=bar" \
       -e "CLOUDWATCH_ENDPOINT=https://localstack:4582" \
       -e "DYNAMODB_ENDPOINT=https://localstack:4569" \
       -e "CERMINE_ENDPOINT=http://cermine:8000" \
       -e "REFEXTRACT_ENDPOINT=http://refextract:8000" \
       -e "DYNAMODB_VERIFY=false" \
       -e "GROBID_PORT=8080" \
       -e "GROBID_HOSTNAME=grobid" \
       -p 8000:8000 \
       -e "LOGLEVEL=10" \
       arxiv/references-api:latest

   # Start the agent.
   docker run -d --network=test \
       -e "MODE=test" \
       -e "AWS_ACCESS_KEY_ID=foo" \
       -e "AWS_SECRET_ACCESS_KEY=bar" \
       -e "JAVA_FLAGS=-Dcom.amazonaws.sdk.disableCertChecking" \
       -e "AWS_CBOR_DISABLE=true" \
       arxiv/references-agent:latest


That's it! Try:

POST to ``http://localhost:8000/references`` with a JSON payload:

.. code-block:: javascript

   {
        "document_id": "1606.00123v3",
        "url": "https://arxiv.org/pdf/1606.00123v3.pdf"
   }

Use `boto3
<http://boto3.readthedocs.io/en/latest/reference/services/kinesis.html>`_ to
put a new record on the ``PDFIsAvailable`` stream. Use the same payload as
above.
